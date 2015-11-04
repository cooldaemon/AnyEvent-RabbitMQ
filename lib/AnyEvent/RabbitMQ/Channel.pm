package AnyEvent::RabbitMQ::Channel;

use strict;
use warnings;

use AnyEvent::RabbitMQ::LocalQueue;
use AnyEvent;
use Scalar::Util qw( looks_like_number weaken );
use Devel::GlobalDestruction;
use Carp qw(croak);
use POSIX qw(ceil);
BEGIN { *Dumper = \&AnyEvent::RabbitMQ::Dumper }

use namespace::clean;

use constant {
    _ST_CLOSED => 0,
    _ST_OPENING => 1,
    _ST_OPEN => 2,
};

sub new {
    my $class = shift;

    my $self = bless {
        on_close       => sub {},
        @_,    # id, connection, on_return, on_close, on_inactive, on_active
        _queue         => AnyEvent::RabbitMQ::LocalQueue->new,
        _content_queue => AnyEvent::RabbitMQ::LocalQueue->new,
    }, $class;
    weaken($self->{connection});
    return $self->_reset;
}

sub _reset {
    my $self = shift;

    my %a = (
        _state         => _ST_CLOSED,
        _is_active     => 0,
        _is_confirm    => 0,
        _publish_tag   => 0,
        _publish_cbs   => {},  # values: [on_ack, on_nack, on_return]
        _consumer_cbs  => {},  # values: [on_consume, on_cancel...]
    );
    @$self{keys %a} = values %a;

    return $self;
}

sub id {
    my $self = shift;
    return $self->{id};
}

sub is_open {
    my $self = shift;
    return $self->{_state} == _ST_OPEN;
}

sub is_active {
    my $self = shift;
    return $self->{_is_active};
}

sub is_confirm {
    my $self = shift;
    return $self->{_is_confirm};
}

sub queue {
    my $self = shift;
    return $self->{_queue};
}

sub open {
    my $self = shift;
    my %args = @_;

    if ($self->{_state} != _ST_CLOSED) {
        $args{on_failure}->('Channel has already been opened');
        return $self;
    }

    $self->{_state} = _ST_OPENING;

    $self->{connection}->_push_write_and_read(
        'Channel::Open', {}, 'Channel::OpenOk',
        sub {
            $self->{_state} = _ST_OPEN;
            $self->{_is_active} = 1;
            $args{on_success}->($self);
        },
        sub {
	    $self->{_state} = _ST_CLOSED;
            $args{on_failure}->($self);
        },
        $self->{id},
    );

    return $self;
}

sub close {
    my $self = shift;
    my $connection = $self->{connection}
        or return;
    my %args = $connection->_set_cbs(@_);

    # If open in in progess, wait for it; 1s arbitrary timing.

    weaken(my $wself = $self);
    my $t; $t = AE::timer 0, 1, sub {
	(my $self = $wself) or undef $t, return;
	return if $self->{_state} == _ST_OPENING;

	# No more tests are required
	undef $t;

        # Double close is OK
	if ($self->{_state} == _ST_CLOSED) {
	    $args{on_success}->($self);
            return;
        }

        $connection->_push_write(
            $self->_close_frame,
            $self->{id},
        );

        # The spec says that after a party sends Channel::Close, it MUST
        # discard all frames for that channel.  So this channel is dead
        # immediately.
        $self->_closed();

        $connection->_push_read_and_valid(
            'Channel::CloseOk',
            sub {
                $args{on_success}->($self);
                $self->_orphan();
            },
            sub {
                $args{on_failure}->(@_);
                $self->_orphan();
            },
            $self->{id},
        );
    };

    return $self;
}

sub _closed {
    my $self = shift;
    my ($frame,) = @_;
    $frame ||= $self->_close_frame();

    return if $self->{_state} == _ST_CLOSED;
    $self->{_state} = _ST_CLOSED;

    # Perform callbacks for all outstanding commands
    $self->{_queue}->_flush($frame);
    $self->{_content_queue}->_flush($frame);

    # Fake nacks of all outstanding publishes
    $_->($frame) for grep { defined } map { $_->[1] } values %{ $self->{_publish_cbs} };

    # Report cancelation of all outstanding consumes
    my @tags = keys %{ $self->{_consumer_cbs} };
    $self->_canceled($_, $frame) for @tags;

    # Report close to on_close callback
    { local $@;
      eval { $self->{on_close}->($frame) };
      warn "Error in channel on_close callback, ignored:\n  $@  " if $@; }

    # Reset state (partly redundant)
    $self->_reset;

    return $self;
}

sub _close_frame {
    my $self = shift;
    my ($text,) = @_;

    Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Channel::Close->new(
            reply_text => $text,
        ),
    );
}

sub _orphan {
    my $self = shift;

    if (my $connection = $self->{connection}) {
        $connection->_delete_channel($self);
    }
    return $self;
}

sub declare_exchange {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Exchange::Declare',
        {
            type        => 'direct',
            passive     => 0,
            durable     => 0,
            auto_delete => 0,
            internal    => 0,
            %args, # exchange
            ticket      => 0,
            nowait      => 0, # FIXME
        },
        'Exchange::DeclareOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub bind_exchange {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Exchange::Bind',
        {
            %args, # source, destination, routing_key
            ticket      => 0,
            nowait      => 0, # FIXME
        },
        'Exchange::BindOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub unbind_exchange {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Exchange::Unbind',
        {
            %args, # source, destination, routing_key
            ticket      => 0,
            nowait      => 0, # FIXME
        },
        'Exchange::UnbindOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub delete_exchange {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Exchange::Delete',
        {
            if_unused => 0,
            %args, # exchange
            ticket    => 0,
            nowait    => 0, # FIXME
        },
        'Exchange::DeleteOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub declare_queue {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Queue::Declare',
        {
            queue       => '',
            passive     => 0,
            durable     => 0,
            exclusive   => 0,
            auto_delete => 0,
            no_ack      => 1,
            %args,
            ticket      => 0,
            nowait      => 0, # FIXME
        },
        'Queue::DeclareOk',
        $cb,
        $failure_cb,
        $self->{id},
    );
}

sub bind_queue {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Queue::Bind',
        {
            %args, # queue, exchange, routing_key
            ticket => 0,
            nowait => 0, # FIXME
        },
        'Queue::BindOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub unbind_queue {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Queue::Unbind',
        {
            %args, # queue, exchange, routing_key
            ticket => 0,
        },
        'Queue::UnbindOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub purge_queue {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Queue::Purge',
        {
            %args, # queue
            ticket => 0,
            nowait => 0, # FIXME
        },
        'Queue::PurgeOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub delete_queue {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Queue::Delete',
        {
            if_unused => 0,
            if_empty  => 0,
            %args, # queue
            ticket    => 0,
            nowait    => 0, # FIXME
        },
        'Queue::DeleteOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub publish {
    my $self = shift;
    my %args = @_;

    # Docs should advise channel-level callback over this, but still, better to give user an out
    unless ($self->{_is_active}) {
        if (defined $args{on_inactive}) {
            $args{on_inactive}->();
            return $self;
        }
        croak "Can't publish on inactive channel (server flow control); provide on_inactive callback";
    }

    my $header_args = delete $args{header};
    my $body        = delete $args{body};
    my $ack_cb      = delete $args{on_ack};
    my $nack_cb     = delete $args{on_nack};
    my $return_cb   = delete $args{on_return};

    defined($header_args) or $header_args = {};
    defined($body) or $body = '';
    defined($ack_cb) or defined($nack_cb) or defined($return_cb)
       and !$self->{_is_confirm}
       and croak "Can't set on_ack/on_nack/on_return callback when not in confirm mode";

    my $tag;
    if ($self->{_is_confirm}) {
        # yeah, delivery tags in acks are sequential.  see Java client
        $tag = ++$self->{_publish_tag};
        if ($return_cb) {
            $header_args = { %$header_args };
            $header_args->{headers}->{_ar_return} = $tag;  # just reuse the same value, why not
        }
        $self->{_publish_cbs}->{$tag} = [$ack_cb, $nack_cb, $return_cb];
    }

    $self->_publish(
        %args,
    )->_header(
        $header_args, $body,
    )->_body(
        $body,
    );

    return $self;
}

sub _publish {
    my $self = shift;
    my %args = @_;

    $self->{connection}->_push_write(
        Net::AMQP::Protocol::Basic::Publish->new(
            exchange  => '',
            mandatory => 0,
            immediate => 0,
            %args, # routing_key
            ticket    => 0,
        ),
        $self->{id},
    );

    return $self;
}

sub _header {
    my ($self, $args, $body) = @_;

    my $weight = delete $args->{weight} || 0;

    # user-provided message headers must be strings.  protect values that look like numbers.
    my $headers = $args->{headers} || {};
    my @prot = grep { my $v = $headers->{$_}; !ref($v) && looks_like_number($v) } keys %$headers;
    if (@prot) {
        $headers = {
            %$headers,
            map { $_ => Net::AMQP::Value::String->new($headers->{$_}) } @prot
        };
    }

    $self->{connection}->_push_write(
        Net::AMQP::Frame::Header->new(
            weight       => $weight,
            body_size    => length($body),
            header_frame => Net::AMQP::Protocol::Basic::ContentHeader->new(
                content_type     => 'application/octet-stream',
                content_encoding => undef,
                delivery_mode    => 1,
                priority         => 1,
                correlation_id   => undef,
                expiration       => undef,
                message_id       => undef,
                timestamp        => time,
                type             => undef,
                user_id          => $self->{connection}->login_user,
                app_id           => undef,
                cluster_id       => undef,
                %$args,
                headers          => $headers,
            ),
        ),
        $self->{id},
    );

    return $self;
}

sub _body {
    my ($self, $body,) = @_;

    my $body_max = $self->{connection}->{_body_max} || length $body;

    # chunk up body into segments measured by $frame_max
    while (length $body) {
        $self->{connection}->_push_write(
            Net::AMQP::Frame::Body->new(
                payload => substr($body, 0, $body_max, '')),
            $self->{id}
        );
    }

    return $self;
}

sub consume {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    my $consumer_cb = delete $args{on_consume}  || sub {};
    my $cancel_cb   = delete $args{on_cancel}   || sub {};
    my $no_ack      = delete $args{no_ack}      // 1;

    $self->{connection}->_push_write_and_read(
        'Basic::Consume',
        {
            consumer_tag => '',
            no_local     => 0,
            no_ack       => $no_ack,
            exclusive    => 0,

            %args, # queue
            ticket       => 0,
            nowait       => 0, # FIXME
        },
        'Basic::ConsumeOk',
        sub {
            my $frame = shift;
            my $tag = $frame->method_frame->consumer_tag;
            $self->{_consumer_cbs}->{$tag} = [ $consumer_cb, $cancel_cb ];
            $cb->($frame);
        },
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub cancel {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    if (!defined $args{consumer_tag}) {
        $failure_cb->('consumer_tag is not set');
        return $self;
    }

    my $cons_cbs = $self->{_consumer_cbs}->{$args{consumer_tag}};
    unless ($cons_cbs) {
        $failure_cb->('Unknown consumer_tag');
        return $self;
    }
    push @$cons_cbs, $cb;

    $self->{connection}->_push_write(
        Net::AMQP::Protocol::Basic::Cancel->new(
            %args, # consumer_tag
            nowait => 0,
        ),
        $self->{id},
    );

    return $self;
}

sub _canceled {
    my $self = shift;
    my ($tag, $frame,) = @_;

    my $cons_cbs = delete $self->{_consumer_cbs}->{$tag}
      or return 0;

    shift @$cons_cbs; # no more deliveries
    for my $cb (reverse @$cons_cbs) {
        $cb->($frame);
    }
    return 1;
}

sub get {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Basic::Get',
        {
            no_ack => 1,
            %args, # queue
            ticket => 0,
        },
        [qw(Basic::GetOk Basic::GetEmpty)],
        sub {
            my $frame = shift;
            return $cb->({empty => $frame})
                if $frame->method_frame->isa('Net::AMQP::Protocol::Basic::GetEmpty');
            $self->_push_read_header_and_body('ok', $frame, $cb, $failure_cb);
        },
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub ack {
    my $self = shift;
    my %args = @_;

    return $self if !$self->_check_open(sub {});

    $self->{connection}->_push_write(
        Net::AMQP::Protocol::Basic::Ack->new(
            delivery_tag => 0,
            multiple     => (
                defined $args{delivery_tag} && $args{delivery_tag} != 0 ? 0 : 1
            ),
            %args,
        ),
        $self->{id},
    );

    return $self;
}

sub qos {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Basic::Qos',
        {
            prefetch_count => 1,
            prefetch_size  => 0,
            global         => 0,
            %args,
        },
        'Basic::QosOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub confirm {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);
    return $self if !$self->_check_version(0, 9, $failure_cb);

    weaken(my $wself = $self);

    $self->{connection}->_push_write_and_read(
        'Confirm::Select',
        {
            %args,
            nowait       => 0, # FIXME
        },
        'Confirm::SelectOk',
        sub {
            my $me = $wself or return;
            $me->{_is_confirm} = 1;
            $cb->();
        },
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub recover {
    my $self = shift;
    my ($cb, $failure_cb, %args) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open(sub {});

    $self->{connection}->_push_write(
        Net::AMQP::Protocol::Basic::Recover->new(
            requeue => 1,
            %args,
        ),
        $self->{id},
    );

     if (!$args{nowait} && $self->_check_version(0, 9)) {
        $self->{connection}->_push_read_and_valid(
            'Basic::RecoverOk',
            $cb,
            $failure_cb,
            $self->{id},
        );
    }
    else {
        $cb->();
    }

    return $self;
}

sub reject {
    my $self = shift;
    my %args = @_;

    return $self if !$self->_check_open( sub { } );

    $self->{connection}->_push_write(
        Net::AMQP::Protocol::Basic::Reject->new(
            delivery_tag => 0,
            requeue      => 0,
            %args,
        ),
        $self->{id},
    );

    return $self;
}

sub select_tx {
    my $self = shift;
    my ($cb, $failure_cb,) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Tx::Select', {}, 'Tx::SelectOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub commit_tx {
    my $self = shift;
    my ($cb, $failure_cb,) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Tx::Commit', {}, 'Tx::CommitOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub rollback_tx {
    my $self = shift;
    my ($cb, $failure_cb,) = $self->_delete_cbs(@_);

    return $self if !$self->_check_open($failure_cb);

    $self->{connection}->_push_write_and_read(
        'Tx::Rollback', {}, 'Tx::RollbackOk',
        $cb,
        $failure_cb,
        $self->{id},
    );

    return $self;
}

sub push_queue_or_consume {
    my $self = shift;
    my ($frame, $failure_cb,) = @_;

    # Note: the spec says that after a party sends Channel::Close, it MUST
    # discard all frames for that channel other than Close and CloseOk.

    if ($frame->isa('Net::AMQP::Frame::Method')) {
        my $method_frame = $frame->method_frame;
        if ($method_frame->isa('Net::AMQP::Protocol::Channel::Close')) {
            $self->{connection}->_push_write(
                Net::AMQP::Protocol::Channel::CloseOk->new(),
                $self->{id},
            );
            $self->_closed($frame);
            $self->_orphan();
            return $self;
        } elsif ($self->{_state} != _ST_OPEN) {
            if ($method_frame->isa('Net::AMQP::Protocol::Channel::OpenOk') ||
                $method_frame->isa('Net::AMQP::Protocol::Channel::CloseOk')) {
                $self->{_queue}->push($frame);
            }
            return $self;
        } elsif ($method_frame->isa('Net::AMQP::Protocol::Basic::Deliver')) {
            my $cons_cbs = $self->{_consumer_cbs}->{$method_frame->consumer_tag};
            my $cb = ($cons_cbs && $cons_cbs->[0]) || sub {};
            $self->_push_read_header_and_body('deliver', $frame, $cb, $failure_cb);
            return $self;
        } elsif ($method_frame->isa('Net::AMQP::Protocol::Basic::CancelOk') ||
                 $method_frame->isa('Net::AMQP::Protocol::Basic::Cancel')) {
            # CancelOk means we asked for a cancel.
            # Cancel means queue was deleted; it is not AMQP, but RMQ supports it.
            if (!$self->_canceled($method_frame->consumer_tag, $frame)
                  && $method_frame->isa('Net::AMQP::Protocol::Basic::CancelOk')) {
                $failure_cb->("Received CancelOk for unknown consumer tag " . $method_frame->consumer_tag);
            }
            return $self;
        } elsif ($method_frame->isa('Net::AMQP::Protocol::Basic::Return')) {
            weaken(my $wself = $self);
            my $cb = sub {
                my $ret = shift;
                my $me = $wself or return;
                my $headers = $ret->{header}->headers || {};
                my $onret_cb;
                if (defined(my $tag = $headers->{_ar_return})) {
                    my $cbs = $me->{_publish_cbs}->{$tag};
                    $onret_cb = $cbs->[2] if $cbs;
                }
                $onret_cb ||= $me->{on_return} || $me->{connection}->{on_return} || sub {};  # oh well
                $onret_cb->($frame);
            };
            $self->_push_read_header_and_body('return', $frame, $cb, $failure_cb);
            return $self;
        } elsif ($method_frame->isa('Net::AMQP::Protocol::Basic::Ack') ||
                 $method_frame->isa('Net::AMQP::Protocol::Basic::Nack')) {
            (my $resp = ref($method_frame)) =~ s/.*:://;
            my $cbs;
            if (!$self->{_is_confirm}) {
                $failure_cb->("Received $resp when not in confirm mode");
            }
            else {
                my @tags;
                if ($method_frame->{multiple}) {
                    @tags = sort { $a <=> $b }
                              grep { $_ <= $method_frame->{delivery_tag} }
                                keys %{$self->{_publish_cbs}};
                }
                else {
                    @tags = ($method_frame->{delivery_tag});
                }
                my $cbi = ($resp eq 'Ack') ? 0 : 1;
                for my $tag (@tags) {
                    my $cbs;
                    if (not $cbs = delete $self->{_publish_cbs}->{$tag}) {
                        $failure_cb->("Received $resp of unknown delivery tag $tag");
                    }
                    elsif ($cbs->[$cbi]) {
                        $cbs->[$cbi]->($frame);
                    }
                }
            }
            return $self;
        } elsif ($method_frame->isa('Net::AMQP::Protocol::Channel::Flow')) {
            $self->{_is_active} = $method_frame->active;
            $self->{connection}->_push_write(
                Net::AMQP::Protocol::Channel::FlowOk->new(
                    active => $method_frame->active,
                ),
                $self->{id},
            );
            my $cbname = $self->{_is_active} ? 'on_active' : 'on_inactive';
            my $cb = $self->{$cbname} || $self->{connection}->{$cbname} || sub {};
            $cb->($frame);
            return $self;
        }
        $self->{_queue}->push($frame);
    } else {
        $self->{_content_queue}->push($frame);
    }

    return $self;
}

sub _push_read_header_and_body {
    my $self = shift;
    my ($type, $frame, $cb, $failure_cb,) = @_;
    my $response = {$type => $frame};
    my $body_size = 0;

    $self->{_content_queue}->get(sub{
        my $frame = shift;

        return $failure_cb->('Received data is not header frame')
            if !$frame->isa('Net::AMQP::Frame::Header');

        my $header_frame = $frame->header_frame;
        return $failure_cb->(
              'Header is not Protocol::Basic::ContentHeader'
            . 'Header was ' . ref $header_frame
        ) if !$header_frame->isa('Net::AMQP::Protocol::Basic::ContentHeader');

        $response->{header} = $header_frame;
        $body_size = $frame->body_size;
    });

    weaken(my $wcontq = $self->{_content_queue});
    my $body_payload = "";
    my $w_next_frame;
    my $next_frame = sub {
        my $frame = shift;

        my $contq = $wcontq or return;

        return $failure_cb->('Received data is not body frame')
            if !$frame->isa('Net::AMQP::Frame::Body');

        $body_payload .= $frame->payload;

        if (length($body_payload) < $body_size) {
            # More to come
            $contq->get($w_next_frame);
        }
        else {
            $frame->payload($body_payload);
            $response->{body} = $frame;
            $cb->($response);
        }
    };
    $w_next_frame = $next_frame;
    weaken($w_next_frame);

    $self->{_content_queue}->get($next_frame);

    return $self;
}

sub _delete_cbs {
    my $self = shift;
    my %args = @_;

    my $cb         = delete $args{on_success} || sub {};
    my $failure_cb = delete $args{on_failure} || sub {die @_};

    return $cb, $failure_cb, %args;
}

sub _check_open {
    my $self = shift;
    my ($failure_cb) = @_;

    return 1 if $self->is_open();

    $failure_cb->('Channel has already been closed');
    return 0;
}

sub _check_version {
    my $self = shift;
    my ($major, $minor, $failure_cb) = @_;

    my $amaj = $Net::AMQP::Protocol::VERSION_MAJOR;
    my $amin = $Net::AMQP::Protocol::VERSION_MINOR;

    return 1 if $amaj >= $major || $amaj == $major && $amin >= $minor;

    $failure_cb->("Not supported in AMQP $amaj-$amin") if $failure_cb;
    return 0;
}

sub DESTROY {
    my $self = shift;
    $self->close() if !in_global_destruction && $self->is_open();
    return;
}

1;
__END__

=head1 NAME

AnyEvent::RabbitMQ::Channel - Abstraction of an AMQP channel.

=head1 SYNOPSIS

    my $ch = $rf->open_channel();
    $ch->declare_exchange(exchange => 'test_exchange');

=head1 DESCRIPTION

A RabbitMQ channel.

A channel is a light-weight virtual connection within a TCP connection to a
RabbitMQ broker.

=head1 ARGUMENTS FOR C<open_channel>

=over

=item on_close

Callback invoked when the channel closes.  Callback will be passed the
incoming message that caused the close, if any.

=item on_return

Callback invoked when a mandatory or immediate message publish fails.
Callback will be passed the incoming message, with accessors
C<method_frame>, C<header_frame>, and C<body_frame>.

=back

=head1 METHODS

=head2 declare_exchange (%args)

Declare an exchange (to publish messages to) on the server.

Arguments:

=over

=item on_success

=item on_failure

=item type

Default 'direct'

=item passive

Default 0

=item durable

Default 0

=item auto_delete

Default 0

=item internal

Default 0

=item exchange

The name of the exchange

=back

=head2 bind_exchange

Binds an exchange to another exchange, with a routing key.

Arguments:

=over

=item source

The name of the source exchange to bind

=item destination

The name of the destination exchange to bind

=item routing_key

The routing key to bind with

=back

=head2 unbind_exchange

=head2 delete_exchange

=head2 declare_queue

Declare a queue, that is, create it if it doesn't exist yet.

Arguments:

=over

=item queue

Name of the queue to be declared. If the queue name is the empty string,
RabbitMQ will create a unique name for the queue. This is useful for
temporary/private reply queues.

=item on_success

Callback that is called when the queue was declared successfully. The argument
to the callback is of type L<Net::AMQP::Frame::Method>. To get the name of the
Queue (if you declared it with an empty name), you can say

    on_success => sub {
        my $method = shift;
        my $name   = $method->method_frame->queue;
    };

=item on_failure

Callback that is called when the declaration of the queue has failed.

=item auto_delete

0 or 1, default 0

=item passive

0 or 1, default 0

=item durable

0 or 1, default 0

=item exclusive

0 or 1, default 0

=item no_ack

0 or 1, default 1

=item ticket

default 0

=back

=head2 bind_queue

Binds a queue to an exchange, with a routing key.

Arguments:

=over

=item queue

The name of the queue to bind

=item exchange

The name of the exchange to bind

=item routing_key

The routing key to bind with

=back

=head2 unbind_queue

=head2 purge_queue

Flushes the contents of a queue.

=head2 delete_queue

Deletes a queue. The queue may not have any active consumers.

=head2 publish

Publish a message to an exchange

Arguments:

=over

=item body

The text body of the message to send.

=item header

Customer headers for the message (if any).

=item exchange

The name of the exchange to send the message to.

=item routing_key

The routing key with which to publish the message.

=item on_ack

Callback (if any) for confirming acknowledgment when in confirm mode.

=back

=head2 consume

Subscribe to consume messages from a queue.

Arguments:

=over

=item queue

The name of the queue to be consumed from.

=item on_consume

Callback called with an argument of the message which has been consumed.

The message is a hash reference, where the value to key C<header> is an object
of type L<Net::AMQP::Protocol::Basic::ContentHeader>, L<body> is a
L<Net::AMQP::Frame::Body>, and C<deliver> a L<Net::AMQP::Frame::Method>.

=item on_cancel

Callback called if consumption is canceled.  This may be at client request
or as a side effect of queue deletion.  (Notification of queue deletion is a
RabbitMQ extension.)

=item consumer_tag

Identifies this consumer, will be auto-generated if you do not provide it, but you must
supply a value if you want to be able to later cancel the subscription.

=item on_success

Callback called if the subscription was successful (before the first message is consumed).

=item on_failure

Callback called if the subscription fails for any reason.

=item no_ack

Pass through the C<no_ack> flag. Defaults to C<1>. If set to C<1>, the server
will not expect messages to be acknowledged.

=back

=head2 publish

Publish a message to an exchange.

Arguments:

=over

=item header

Hash of AMQP message header info, including the confusingly similar element "headers",
which may contain arbitrary string key/value pairs.

=item body

Message body.

=item mandatory

Boolean; if true, then if the message doesn't land in a queue (e.g. the exchange has no
bindings), it will be "returned."  (see "on_return")

=item immediate

Boolean; if true, then if the message cannot be delivered directly to a consumer, it
will be "returned."  (see "on_return")

=item on_ack

Callback called with the frame that acknowledges receipt (if channel is in confirm mode),
typically L<Net::AMQP::Protocol::Basic::Ack>.

=item on_nack

Callback called with the frame that declines receipt (if the channel is in confirm mode),
typically L<Net::AMQP::Protocol::Basic::Nack> or L<Net::AMQP::Protocol::Channel::Close>.

=item on_return

In AMQP, a "returned" message is one that cannot be delivered in compliance with the
C<immediate> or C<mandatory> flags.

If in confirm mode, this callback will be called with the frame that reports message
return, typically L<Net::AMQP::Protocol::Basic::Return>.  If confirm mode is off or
this callback is not provided, then the channel or connection objects' on_return
callbacks (if any), will be called instead.

NOTE: If confirm mode is on, the on_ack or on_nack callback will be called whether or
not on_return is called first.

=back

=head2 cancel

Cancel a queue subscription.

Note that the cancellation B<will not> take place at once, and further messages may be
consumed before the subscription is cancelled. No further messages will be
consumed after the on_success callback has been called.

Arguments:

=over

=item consumer_tag

Identifies this consumer, needs to be the value supplied when the queue is initially
consumed from.

=item on_success

Callback called if the subscription was successfully cancelled.

=item on_failure

Callback called if the subscription could not be cancelled for any reason.

=back

=head2 get

Try to get a single message from a queue.

Arguments:

=over

=item queue

Mandatory. Name of the queue to try to receive a message from.

=item on_success

Will be called either with either a message, or, if the queue is empty,
a notification that there was nothing to collect from the queue.

=item on_failure

This callback will be called if an error is signalled on this channel.

=back

=head2 ack

=head2 qos

=head2 confirm

Put channel into confirm mode.  In confirm mode, publishes are confirmed by
the server, so the on_ack callback of publish works.

=head2 recover

=head2 select_tx

=head2 commit_tx

=head2 rollback_tx

=head1 AUTHOR, COPYRIGHT AND LICENSE

See L<AnyEvent::RabbitMQ> for author(s), copyright and license.

=cut
