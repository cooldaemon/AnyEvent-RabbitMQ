package AnyEvent::RabbitMQ;

use strict;
use warnings;
use Carp qw(confess croak);
use Scalar::Util qw(refaddr);
use List::MoreUtils qw(none);
use Devel::GlobalDestruction;
use File::ShareDir;
use Readonly;
use Scalar::Util qw/ weaken /;

require Data::Dumper;
sub Dumper {
    local $Data::Dumper::Terse = 1;
    local $Data::Dumper::Indent = 1;
    local $Data::Dumper::Useqq = 1;
    local $Data::Dumper::Deparse = 1;
    local $Data::Dumper::Quotekeys = 0;
    local $Data::Dumper::Sortkeys = 1;
    &Data::Dumper::Dumper
}

use AnyEvent::Handle;
use AnyEvent::Socket;

use Net::AMQP 0.06;
use Net::AMQP::Common qw(:all);

use AnyEvent::RabbitMQ::Channel;
use AnyEvent::RabbitMQ::LocalQueue;

use namespace::clean;

our $VERSION = '1.16';

use constant {
    _ST_CLOSED => 0,
    _ST_OPENING => 1,
    _ST_OPEN => 2,
    _ST_CLOSING => 3,
};

Readonly my $DEFAULT_AMQP_SPEC
    => File::ShareDir::dist_dir("AnyEvent-RabbitMQ") . '/fixed_amqp0-9-1.xml';

Readonly my $DEFAULT_CHANNEL_MAX => 2**16;

sub new {
    my $class = shift;
    return bless {
        verbose            => 0,
        @_,
        _state             => _ST_CLOSED,
        _queue             => AnyEvent::RabbitMQ::LocalQueue->new,
        _last_chan_id      => 0,
        _channels          => {},
        _login_user        => '',
        _server_properties => {},
        _frame_max         => undef,
        _body_max          => undef,
        _channel_max       => undef,
    }, $class;
}

sub verbose {
    my $self = shift;
    @_ ? ($self->{verbose} = shift) : $self->{verbose}
}

sub is_open {
    my $self = shift;
    $self->{_state} == _ST_OPEN
}

sub channels {
    my $self = shift;
    return $self->{_channels};
}

sub _delete_channel {
    my $self = shift;
    my ($channel,) = @_;
    my $c = $self->{_channels}->{$channel->id};
    if (defined($c) && refaddr($c) == refaddr($channel)) {
        delete $self->{_channels}->{$channel->id};
        return 1;
    }
    return 0;
}

sub login_user {
    my $self = shift;
    return $self->{_login_user};
}

my $_loaded_spec;
sub load_xml_spec {
    my $self = shift;
    my ($spec) = @_;
    $spec ||= $DEFAULT_AMQP_SPEC;
    if ($_loaded_spec && $_loaded_spec ne $spec) {
        croak("Tried to load AMQP spec $spec, but have already loaded $_loaded_spec, not possible");
    }
    elsif (!$_loaded_spec) {
        Net::AMQP::Protocol->load_xml_spec($_loaded_spec = $spec);
    }
    return $self;
}

sub connect {
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    if ($self->{_state} != _ST_CLOSED) {
        $args{on_failure}->('Connection has already been opened');
        return $self;
    }

    $args{on_close}        ||= sub {};
    $args{on_read_failure} ||= sub {warn @_, "\n"};
    $args{timeout}         ||= 0;

    for (qw/ host port /) {
        $args{$_} or return $args{on_failure}->("No $_ passed to connect");
    }

    if ($self->{verbose}) {
        warn 'connect to ', $args{host}, ':', $args{port}, '...', "\n";
    }

    $self->{_state} = _ST_OPENING;

    weaken(my $weak_self = $self);
    my $conn; $conn = AnyEvent::Socket::tcp_connect(
        $args{host},
        $args{port},
        sub {
            undef $conn;
            my $self = $weak_self or return;

            my $fh = shift or return $args{on_failure}->(
                sprintf('Error connecting to AMQP Server %s:%s: %s', $args{host}, $args{port}, $!)
            );

            my $close_cb = $args{on_close};
            my $failure_cb = $args{on_failure};
            $self->{_handle} = AnyEvent::Handle->new(
                fh       => $fh,
                on_error => sub {
                    my ($handle, $fatal, $message) = @_;
                    my $self = $weak_self or return;

                    if ($self->is_open) {
                        $self->_server_closed($close_cb, $message);
                    }
                    else {
                        $failure_cb->(@_);
                    }
                },
                on_drain => sub {
                    my ($handle) = @_;
                    my $self = $weak_self or return;

                    $self->{drain_condvar}->send
                        if exists $self->{drain_condvar};
                },
                $args{tls} ? (tls => 'connect') : (),
            );
            $self->_read_loop($args{on_close}, $args{on_read_failure});
            $self->_start(%args,);
        },
        sub {
            return $args{timeout};
        },
    );

    return $self;
}

sub server_properties {
    return shift->{_server_properties};
}

sub _read_loop {
    my ($self, $close_cb, $failure_cb,) = @_;

    return if !defined $self->{_handle}; # called on_error

    weaken(my $weak_self = $self);
    $self->{_handle}->push_read(chunk => 8, sub {
        my $self = $weak_self or return;
        my $data = $_[1];
        my $stack = $_[1];

        if (length($data) <= 7) {
            $failure_cb->('Broken data was received');
            @_ = ($self, $close_cb, $failure_cb,);
            goto &_read_loop;
        }

        my ($type_id, $channel, $length,) = unpack 'CnN', substr $data, 0, 7, '';
        if (!defined $type_id || !defined $channel || !defined $length) {
            $failure_cb->('Broken data was received');
            @_ = ($self, $close_cb, $failure_cb,);
            goto &_read_loop;
        }

        $self->{_handle}->push_read(chunk => $length, sub {
            my $self = $weak_self or return;
            $stack .= $_[1];
            my ($frame) = Net::AMQP->parse_raw_frames(\$stack);

            $self->{_heartbeat_recv} = time if $self->{_heartbeat_timer};

            if ($self->{verbose}) {
                warn '[C] <-- [S] ', Dumper($frame),
                     '-----------', "\n";
            }

            my $id = $frame->channel;
            if (0 == $id) {
                if ($frame->type_id == 8) {
                    # Heartbeat, no action needs taking.
                }
                else {
                    return unless $self->_check_close_and_clean($frame, $close_cb,);
                    $self->{_queue}->push($frame);
                }
            } else {
                my $channel = $self->{_channels}->{$id};
                if (defined $channel) {
                    $channel->push_queue_or_consume($frame, $failure_cb);
                } else {
                    $failure_cb->('Unknown channel id: ' . $frame->channel);
                }
            }

            @_ = ($self, $close_cb, $failure_cb,);
            goto &_read_loop;
        });
    });

    return $self;
}

sub _check_close_and_clean {
    my $self = shift;
    my ($frame, $close_cb,) = @_;

    my $method_frame = $frame->isa('Net::AMQP::Frame::Method') ? $frame->method_frame : undef;

    if ($self->{_state} == _ST_CLOSED) {
        return $method_frame && $method_frame->isa('Net::AMQP::Protocol::Connection::CloseOk');
    }

    if ($method_frame && $method_frame->isa('Net::AMQP::Protocol::Connection::Close')) {
        delete $self->{_heartbeat_timer};
        $self->_push_write(Net::AMQP::Protocol::Connection::CloseOk->new());
        $self->_server_closed($close_cb, $frame);
        return;
    }

    return 1;
}

sub _server_closed {
    my $self = shift;
    my ($close_cb, $why,) = @_;

    $self->{_state} = _ST_CLOSING;
    for my $channel (values %{ $self->{_channels} }) {
        $channel->_closed(ref($why) ? $why : $channel->_close_frame($why));
    }
    $self->{_channels} = {};
    $self->{_handle}->push_shutdown;
    $self->{_state} = _ST_CLOSED;

    $close_cb->($why);
    return;
}

sub _start {
    my $self = shift;
    my %args = @_;

    if ($self->{verbose}) {
        warn 'post header', "\n";
    }

    $self->{_handle}->push_write(Net::AMQP::Protocol->header);

    $self->_push_read_and_valid(
        'Connection::Start',
        sub {
            my $frame = shift;

            my @mechanisms = split /\s/, $frame->method_frame->mechanisms;
            return $args{on_failure}->('AMQPLAIN is not found in mechanisms')
                if none {$_ eq 'AMQPLAIN'} @mechanisms;

            my @locales = split /\s/, $frame->method_frame->locales;
            return $args{on_failure}->('en_US is not found in locales')
                if none {$_ eq 'en_US'} @locales;

            $self->{_server_properties} = $frame->method_frame->server_properties;

            $self->_push_write(
                Net::AMQP::Protocol::Connection::StartOk->new(
                    client_properties => {
                        platform     => 'Perl',
                        product      => __PACKAGE__,
                        information  => 'http://d.hatena.ne.jp/cooldaemon/',
                        version      => Net::AMQP::Value::String->new(__PACKAGE__->VERSION),
                        capabilities => {
                            consumer_cancel_notify     => Net::AMQP::Value::true,
                            exchange_exchange_bindings => Net::AMQP::Value::true,
                        },
                        %{ $args{client_properties} || {} },
                    },
                    mechanism => 'AMQPLAIN',
                    response => {
                        LOGIN    => $args{user},
                        PASSWORD => $args{pass},
                    },
                    locale => 'en_US',
                ),
            );

            $self->_tune(%args,);
        },
        $args{on_failure},
    );

    return $self;
}

sub _tune {
    my $self = shift;
    my %args = @_;

    weaken(my $weak_self = $self);
    $self->_push_read_and_valid(
        'Connection::Tune',
        sub {
            my $self = $weak_self or return;
            my $frame = shift;

            my %tune;
            foreach (qw( channel_max frame_max heartbeat )) {
                my $client = $args{tune}{$_} || 0;
                my $server = $frame->method_frame->$_ || 0;

                # negotiate with the server such that we cannot request a larger
                # value set by the server, unless the server said unlimited
                $tune{$_} = ($server == 0 or $client == 0)
                    ? ($server > $client ? $server : $client)   # max
                    : ($client > $server ? $server : $client);  # min
            }

            if ($self->{_frame_max} = $tune{frame_max}) {
                # calculate how big the body can actually be
                $self->{_body_max} = $self->{_frame_max} - Net::AMQP::_HEADER_LEN - Net::AMQP::_FOOTER_LEN;
            }

            $self->{_channel_max} = $tune{channel_max} || $DEFAULT_CHANNEL_MAX;

            $self->_push_write(
                Net::AMQP::Protocol::Connection::TuneOk->new(%tune,)
            );

            if ($tune{heartbeat} > 0) {
                $self->_start_heartbeat($tune{heartbeat}, %args,);
            }

            $self->_open(%args,);
        },
        $args{on_failure},
    );

    return $self;
}

sub _start_heartbeat {
    my ($self, $interval, %args,) = @_;

    my $close_cb   = $args{on_close};
    my $failure_cb = $args{on_read_failure};
    my $last_recv = 0;
    my $idle_cycles = 0;
    weaken(my $weak_self = $self);
    my $timer_cb = sub {
        my $self = $weak_self or return;
        if ($self->{_heartbeat_recv} != $last_recv) {
            $last_recv = $self->{_heartbeat_recv};
            $idle_cycles = 0;
        }
        elsif (++$idle_cycles > 1) {
            delete $self->{_heartbeat_timer};
            $failure_cb->("Heartbeat lost");
            $self->_server_closed($close_cb, "Heartbeat lost");
            return;
        }
        $self->_push_write(Net::AMQP::Frame::Heartbeat->new());
    };

    $self->{_heartbeat_recv} = time;
    $self->{_heartbeat_timer} = AnyEvent->timer(
        after    => $interval,
        interval => $interval,
        cb       => $timer_cb,
    );

    return $self;
}

sub _open {
    my $self = shift;
    my %args = @_;

    $self->_push_write_and_read(
        'Connection::Open',
        {
            virtual_host => $args{vhost},
            insist       => 1,
        },
        'Connection::OpenOk',
        sub {
            $self->{_state} = _ST_OPEN;
            $self->{_login_user} = $args{user};
            $args{on_success}->($self);
        },
        $args{on_failure},
    );

    return $self;
}

sub close {
    return if in_global_destruction;
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    if ($self->{_state} == _ST_CLOSED) {
        $args{on_success}->(@_);
        return $self;
    }
    if ($self->{_state} != _ST_OPEN) {
        $args{on_failure}->(($self->{_state} == _ST_OPENING ? "open" : "close") . " already in progress");
        return $self;
    }
    $self->{_state} = _ST_CLOSING;

    my $cv = AE::cv {
        delete $self->{_closing};
        $self->_finish_close(%args);
    };

    $cv->begin();

    my @ids = keys %{$self->{_channels}};
    for my $id (@ids) {
         my $channel = $self->{_channels}->{$id};
         if ($channel->is_open) {
             $cv->begin();
             $channel->close(
                 on_success => sub { $cv->end() },
                 on_failure => sub { $cv->end() },
             );
         }
    }

    $cv->end();

    return $self;
}

sub _finish_close {
    my $self = shift;
    my %args = @_;

    if (my @ch = map { $_->id } grep { defined() && $_->is_open } values %{$self->{_channels}}) {
        $args{on_failure}->("BUG: closing with channel(s) open: @ch");
        return;
    }

    $self->{_state} = _ST_CLOSED;

    $self->_push_write_and_read(
        'Connection::Close', {}, 'Connection::CloseOk',
        sub {
            # circular ref ok
            $self->{_handle}->push_shutdown;
            $args{on_success}->(@_);
        },
        sub {
            # circular ref ok
            $self->{_handle}->push_shutdown;
            $args{on_failure}->(@_);
        },
    );

    return;
}

sub open_channel {
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    return $self if !$self->_check_open($args{on_failure});

    $args{on_close} ||= sub {};

    my $id = $args{id};
    if ($id && $self->{_channels}->{$id}) {
        $args{on_failure}->("Channel id $id is already in use");
        return $self;
    }

    if (!$id) {
        my $try_id = $self->{_last_chan_id};
        for (1 .. $self->{_channel_max}) {
            $try_id = 1 if ++$try_id > $self->{_channel_max};
            unless (defined $self->{_channels}->{$try_id}) {
                $id = $try_id;
                last;
            }
        }
        if (!$id) {
            $args{on_failure}->('Ran out of channel ids');
            return $self;
        }
        $self->{_last_chan_id} = $id;
    }

    my $channel = AnyEvent::RabbitMQ::Channel->new(
        id         => $id,
        connection => $self,
        on_close   => $args{on_close},
    );

    $self->{_channels}->{$id} = $channel;

    $channel->open(
        on_success => sub {
            $args{on_success}->($channel);
        },
        on_failure => sub {
            $self->_delete_channel($channel);
            $args{on_failure}->(@_);
        },
    );

    return $self;
}

sub _push_write_and_read {
    my $self = shift;
    my ($method, $args, $exp, $cb, $failure_cb, $id,) = @_;

    $method = 'Net::AMQP::Protocol::' . $method;
    $self->_push_write(
        Net::AMQP::Frame::Method->new(
            method_frame => $method->new(%$args)
        ),
        $id,
    );

    return $self->_push_read_and_valid($exp, $cb, $failure_cb, $id,);
}

sub _push_read_and_valid {
    my $self = shift;
    my ($exp, $cb, $failure_cb, $id,) = @_;
    $exp = ref($exp) eq 'ARRAY' ? $exp : [$exp];

    my $queue;
    if (!$id) {
        $queue = $self->{_queue};
    } elsif (defined $self->{_channels}->{$id}) {
        $queue = $self->{_channels}->{$id}->queue;
    } else {
        $failure_cb->('Unknown channel id: ' . $id);
    }

    return unless $queue; # Can go away in global destruction..
    $queue->get(sub {
        my $frame = shift;

        return $failure_cb->('Received data is not method frame')
            if !$frame->isa('Net::AMQP::Frame::Method');

        my $method_frame = $frame->method_frame;
        for my $exp_elem (@$exp) {
            return $cb->($frame)
                if $method_frame->isa('Net::AMQP::Protocol::' . $exp_elem);
        }

        $failure_cb->(
            $method_frame->isa('Net::AMQP::Protocol::Channel::Close')
              ? 'Channel closed'
              : 'Expected ' . join(',', @$exp) . ' but got ' . ref($method_frame)
        );
    });
}

sub _push_write {
    my $self = shift;
    my ($output, $id,) = @_;

    if ($output->isa('Net::AMQP::Protocol::Base')) {
        $output = $output->frame_wrap;
    }
    $output->channel($id || 0);

    if ($self->{verbose}) {
        warn '[C] --> [S] ', Dumper($output);
    }

    $self->{_handle}->push_write($output->to_raw_frame())
        if $self->{_handle}; # Careful - could have gone (global destruction)
    return;
}

sub _set_cbs {
    my $self = shift;
    my %args = @_;

    $args{on_success} ||= sub {};
    $args{on_failure} ||= sub { die @_ unless in_global_destruction };

    return %args;
}

sub _check_open {
    my $self = shift;
    my ($failure_cb) = @_;

    return 1 if $self->is_open;

    $failure_cb->('Connection has already been closed');
    return 0;
}

sub drain_writes {
    my ($self, $timeout) = shift;
    $self->{drain_condvar} = AnyEvent->condvar;
    if ($timeout) {
        $self->{drain_timer} = AnyEvent->timer( after => $timeout, sub {
            $self->{drain_condvar}->croak("Timed out after $timeout");
        });
    }
    $self->{drain_condvar}->recv;
    delete $self->{drain_timer};
}

sub DESTROY {
    my $self = shift;
    $self->close() unless in_global_destruction;
    return;
}

1;
__END__

=head1 NAME

AnyEvent::RabbitMQ - An asynchronous and multi channel Perl AMQP client.

=head1 SYNOPSIS

  use AnyEvent::RabbitMQ;

  my $cv = AnyEvent->condvar;

  my $ar = AnyEvent::RabbitMQ->new->load_xml_spec()->connect(
      host       => 'localhost',
      port       => 5672,
      user       => 'guest',
      pass       => 'guest',
      vhost      => '/',
      timeout    => 1,
      tls        => 0, # Or 1 if you'd like SSL
      tune       => { heartbeat => 30, channel_max => $whatever, frame_max = $whatever },
      on_success => sub {
          my $ar = shift;
          $ar->open_channel(
              on_success => sub {
                  my $channel = shift;
                  $channel->declare_exchange(
                      exchange   => 'test_exchange',
                      on_success => sub {
                          $cv->send('Declared exchange');
                      },
                      on_failure => $cv,
                  );
              },
              on_failure => $cv,
              on_close   => sub {
                  my $method_frame = shift->method_frame;
                  die $method_frame->reply_code, $method_frame->reply_text;
              },
          );
      },
      on_failure => $cv,
      on_read_failure => sub { die @_ },
      on_return  => sub {
          my $frame = shift;
          die "Unable to deliver ", Dumper($frame);
      },
      on_close   => sub {
          my $why = shift;
          if (ref($why)) {
              my $method_frame = $why->method_frame;
              die $method_frame->reply_code, ": ", $method_frame->reply_text;
          }
          else {
              die $why;
          }
      },
  );

  print $cv->recv, "\n";

=head1 DESCRIPTION

AnyEvent::RabbitMQ is an AMQP(Advanced Message Queuing Protocol) client library, that is intended to allow you to interact with AMQP-compliant message brokers/servers such as RabbitMQ in an asynchronous fashion.

You can use AnyEvent::RabbitMQ to -

  * Declare and delete exchanges
  * Declare, delete, bind and unbind queues
  * Set QoS and confirm mode
  * Publish, consume, get, ack, recover and reject messages
  * Select, commit and rollback transactions

AnyEvent::RabbitMQ is known to work with RabbitMQ versions 2.5.1 and versions 0-8 and 0-9-1 of the AMQP specification.

This client is the non-blocking version, for a blocking version with a similar API, see L<Net::RabbitFoot>.

=head1 AUTHOR

Masahito Ikuta E<lt>cooldaemon@gmail.comE<gt>

=head1 MAINTAINER

Currently maintained by C<< <bobtfish@bobtfish.net> >> due to the original
author being missing in action.

=head1 COPYRIGHT

Copyright (c) 2010, the above named author(s).

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

