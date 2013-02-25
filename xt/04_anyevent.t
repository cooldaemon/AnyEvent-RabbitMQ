use Test::More;
use Test::Exception;
use Data::Dumper;

use FindBin;

my %server = (
    product => undef,
    version => undef,
);

my %conf = (
    host  => 'localhost',
    port  => 5672,
    user  => 'guest',
    pass  => 'guest',
    vhost => '/',
#    verbose => 1,
);

eval {
    use IO::Socket::INET;

    my $socket = IO::Socket::INET->new(
        Proto    => 'tcp',
        PeerAddr => $conf{host},
        PeerPort => $conf{port},
        Timeout  => 1,
    ) or die 'Error connecting to AMQP Server!';

    close $socket;
};

plan skip_all => 'Connection failure: '
               . $conf{host} . ':' . $conf{port} if $@;

use AnyEvent::RabbitMQ;

my $ar = AnyEvent::RabbitMQ->new(verbose => $conf{verbose});

lives_ok sub {
    $ar->load_xml_spec()
}, 'load xml spec';

my $done = AnyEvent->condvar;
$ar->connect(
    (map {$_ => $conf{$_}} qw(host port user pass vhost)),
    timeout    => 1,
    on_success => sub {
        my $ar = shift;
        isa_ok($ar, 'AnyEvent::RabbitMQ');
        $server{product} = $ar->server_properties->{product};
        $server{version} = version->parse($ar->server_properties->{version});
        $done->send;
    },
    on_failure => failure_cb($done),
    on_return  => sub {
        my $method_frame = shift->method_frame;
        die "return: ", $method_frame->reply_code, $method_frame->reply_text
          if $method_frame->reply_code;
    },
    on_close   => sub {
        my $method_frame = shift->method_frame;
        Carp::confess "close: ", $method_frame->reply_code, $method_frame->reply_text
          if $method_frame->reply_code;
    },
);
$done->recv;

my $ch;
$done = AnyEvent->condvar;
open_ch($done);
$done->recv;

sub open_ch {
    my ($cv,) = @_;
    $ar->open_channel(
    on_success => sub {
            $ch = shift;
            isa_ok($ch, 'AnyEvent::RabbitMQ::Channel');
            $cv->send;
        },
        on_failure => failure_cb($cv),
        on_close   => sub {
            my $method_frame = shift->method_frame;
            die $method_frame->reply_code, $method_frame->reply_text
              if $method_frame->reply_code;
        },
    );
}

$done = AnyEvent->condvar;
$ch->declare_exchange(
    exchange   => 'test_x',
    on_success => sub {
        pass('declare exchange');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->declare_queue(
    queue      => 'test_q',
    on_success => sub {
        pass('declare queue');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->bind_queue(
    queue       => 'test_q',
    exchange    => 'test_x',
    routing_key => 'test_r',
    on_success  => sub {
        pass('bound queue');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
my $consumer_tag;
$ch->consume(
    queue      => 'test_q',
    on_success => sub {
        my $frame = shift;
        $consumer_tag = $frame->method_frame->consumer_tag;
        pass('consume');
    },
    on_consume => sub {
        my $response = shift;
        ok($response->{body}->payload, 'publish');
        $done->send;
    },
    on_failure => failure_cb($done),
);
publish($ch, 'Hello RabbitMQ.', $done,);
$done->recv;

$done = AnyEvent->condvar;
$ch->cancel(
    consumer_tag => $consumer_tag,
    on_success   => sub {
        pass('cancel');
        $done->send;
    },
    on_failure   => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
publish($ch, 'I love RabbitMQ.', $done,);
$ch->get(
    queue      => 'test_q',
    on_success => sub {
        my $response = shift;
        ok(defined $response->{ok}, 'getok');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->get(
    queue      => 'test_q',
    on_success => sub {
        my $response = shift;
        ok(defined $response->{empty}, 'empty');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

for my $size (10, 131_064, 10) {
    send_large_size_message($ch, $size);
}

$done = AnyEvent->condvar;
$ch->consume(
    queue      => 'test_q',
    no_ack     => 0,
    on_consume => sub {
        my $response = shift;
        $ch->ack(
            delivery_tag => $response->{deliver}->method_frame->delivery_tag
        );
        pass('ack deliver');

        $ch->cancel(
            consumer_tag => $response->{deliver}->method_frame->consumer_tag,
            on_success   => sub {
                pass('cancel');
                $done->send;
            },
            on_failure   => failure_cb($done),
        );
    },
    on_failure => failure_cb($done),
);
publish($ch, 'NO RabbitMQ, NO LIFE.', $done,);
$done->recv;

$done = AnyEvent->condvar;
publish($ch, 'RabbitMQ is cool.', $done,);
$ch->get(
    queue      => 'test_q',
    no_ack     => 0,
    on_success => sub {
        my $response = shift;
        $ch->ack(
            delivery_tag => $response->{ok}->method_frame->delivery_tag
        );
        pass('ack get');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
my @responses;
$ch->qos(
    prefetch_count => 2,
    on_success => sub {
        $ch->consume(
            queue      => 'test_q',
            no_ack     => 0,
            on_consume => sub {
                my $response = shift;
                push @responses, $response;
                return if 2 > scalar @responses;
                $done->send;
            },
            on_failure => failure_cb($done),
        );
    },
    on_failure => failure_cb($done),
);
publish($ch, 'RabbitMQ is excellent.', $done,);
publish($ch, 'RabbitMQ is fantastic.', $done,);
$done->recv;
pass('qos');

for my $response (@responses) {
    $ch->ack(
        delivery_tag => $response->{deliver}->method_frame->delivery_tag,
    );
}

$done = AnyEvent->condvar;
$ch->cancel(
    consumer_tag => $responses[0]->{deliver}->method_frame->consumer_tag,
    on_success   => sub {
        $ch->qos(
            on_success => sub {
                $done->send;
            },
            on_failure => failure_cb($done),
        );
    },
    on_failure   => failure_cb($done),
);
$done->recv;
 
$done = AnyEvent->condvar;
my $recover_count = 0;
$ch->consume(
    queue      => 'test_q',
    no_ack     => 0,
    on_consume => sub {
        my $response = shift;

        if (5 > ++$recover_count) {
            $ch->recover();
            return;
        }

        $ch->ack(
            delivery_tag => $response->{deliver}->method_frame->delivery_tag
        );

        $ch->cancel(
            consumer_tag => $response->{deliver}->method_frame->consumer_tag,
            on_success   => sub {
                $done->send;
            },
            on_failure   => failure_cb($done),
        );
    },
    on_failure => failure_cb($done),
);
publish($ch, 'RabbitMQ is powerful.', $done,);
$done->recv;
pass('recover');

# This only works for RabbitMQ >= 2.0.0
my $can_reject = $server{product} eq 'RabbitMQ' && $server{version} >= version->parse('2.0.0');
SKIP: {
    skip 'We need RabbitMQ >= 2.0.0 for the confirm and reject test', 1 unless $can_reject;

    $done = AnyEvent->condvar;
    $ch->confirm(
    on_success => sub { $done->send },
        on_failure => failure_cb($done),
    );
    $done->recv;
    pass('confirm');

    $done = AnyEvent->condvar;
    my $reject_count = 0;
    $ch->consume(
        queue      => 'test_q',
        no_ack     => 0,
        on_consume => sub {
            my $response = shift;

            if ( 5 > ++$reject_count ) {
                $ch->reject(
                    delivery_tag => $response->{deliver}->method_frame->delivery_tag,

                    # requeue! Else the server does not deliver the message again to this client.
                    requeue => 1,
                );
                return;
            }

            $ch->ack( delivery_tag => $response->{deliver}->method_frame->delivery_tag );

            $ch->cancel(
                consumer_tag => $response->{deliver}->method_frame->consumer_tag,
                on_success   => sub {
                    $done->send;
                },
                on_failure => failure_cb($done),
            );
        },
        on_failure => failure_cb($done),
    );
    my $pub_done = AnyEvent->condvar;
    publish($ch, 'RabbitMQ is powerful.', $pub_done,);
    $pub_done->recv;
    $done->recv;
    pass('reject');

    # reopen because confirm is not compatible with transactions
    $done = AnyEvent->condvar;
    $ch->close(
        on_success => sub {
            pass('close2');
            $done->send;
        },
        on_failure => failure_cb($done),
    );
    $done->recv;
    undef $ch;

    $done = AnyEvent->condvar;
    open_ch($done);
    $done->recv;
    pass('open2');
};

$done = AnyEvent->condvar;
$ch->select_tx(
    on_success => sub {
        pass('select tx');
        publish($ch, 'RabbitMQ is highly reliable systems.', $done,);

        $ch->rollback_tx(
            on_success => sub {
                pass('rollback tx');
                publish($ch, 'RabbitMQ is highly reliable systems.', $done,);

                $ch->commit_tx(
                    on_success => sub {
                        pass('commit tx');
                        $done->send;
                    },
                    on_failure => failure_cb($done),
                );
            },
            on_failure => failure_cb($done),
        );
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->purge_queue(
    queue      => 'test_q',
    on_success => sub {
        pass('purge queue');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->unbind_queue(
    queue       => 'test_q',
    exchange    => 'test_x',
    routing_key => 'test_r',
    on_success  => sub {
        pass('unbind queue');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->delete_queue(
    queue      => 'test_q',
    on_success => sub {
        pass('delete queue');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ch->delete_exchange(
    exchange   => 'test_x',
    on_success => sub {
        pass('delete exchange');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

$done = AnyEvent->condvar;
$ar->close(
    on_success => sub {
        pass('close2');
        $done->send;
    },
    on_failure => failure_cb($done),
);
$done->recv;

sub failure_cb {
    my ($cv,) = @_;
    return sub {
        fail(join(' ', 'on_failure:', @_));
        $cv->send;
    };
}

sub publish {
    my ($ch, $message, $cv,) = @_;

    $ch->publish(
        exchange    => 'test_x',
        routing_key => 'test_r',
        body        => $message,
        on_ack      => sub { $cv->send },
        on_return   => sub {
            my $response = shift;
            fail('on_return: ' . Dumper($response));
            $cv->send;
        },
    );

    return;
}

sub send_large_size_message {
    my ($ch, $size,) = @_;

    my $done = AnyEvent->condvar;
    publish($ch, 'a' x $size, $done,);
    $ch->get(
        queue      => 'test_q',
        on_success => sub {
            my $response = shift;
            is(length($response->{body}->payload), $size, 'get large size: ' . $size);
            $done->send;
        },
        on_failure => failure_cb($done),
    );
    $done->recv;
    return;
}

done_testing;

