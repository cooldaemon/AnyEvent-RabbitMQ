use Test::More;
use Test::Exception;

my %conf = (
    host  => 'localhost',
    port  => 5672,
    user  => 'guest',
    pass  => 'guest',
    vhost => '/',
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
plan tests => 1;

use AnyEvent::RabbitMQ;

my $ar = connect_ar();

my @queues = map {
    my $ch = open_channel($ar);
    my $queue = 'test_q' . $_;
    declare_queue($ch, $queue,);

    my $done = AnyEvent->condvar;
    consume($ch, $queue, sub {
        my $response = shift;
        return if 'stop' ne $response->{body}->payload;
        $done->send();
    });
    {name => $queue, cv => $done};
} (1..5);

my $ch = open_channel($ar);
for my $queue (@queues) {
    publish($ch, $queue->{name}, 'hello');
    publish($ch, $queue->{name}, 'stop');
}

my $count = 0;
for my $queue (@queues) {
    $queue->{cv}->recv;
    $count++;
}

is($count, 5, 'consume count');

for my $queue (@queues) {
    delete_queue($ch, $queue->{name});
}

close_ar($ar);

sub connect_ar {
    my $done = AnyEvent->condvar;
    my $ar = AnyEvent::RabbitMQ->new()->load_xml_spec()->connect(
        (map {$_ => $conf{$_}} qw(host port user pass vhost)),
        timeout    => 1,
        on_success => sub {$done->send(1)},
        on_failure => sub {$done->send()},
        on_close   => \&handle_close,
    );
    die 'Connection failure' if !$done->recv;
    return $ar;
}

sub close_ar {
    my ($ar,) = @_;

    my $done = AnyEvent->condvar;
    $ar->close(
        on_success => sub {$done->send(1)},
        on_failure => sub {$done->send()},
    );
    die 'Close failure' if !$done->recv;

    return;
}

sub open_channel {
    my ($ar,) = @_;
    
    my $done = AnyEvent->condvar;
    $ar->open_channel(
        on_success => sub {$done->send(shift)},
        on_failure => sub {$done->send()},
        on_close   => \&handle_close,
    );
    my $ch = $done->recv;
    die 'Open channel failure' if !$ch;

    return $ch;
}

sub declare_queue {
    my ($ch, $queue,) = @_;

    my $done = AnyEvent->condvar;
    $ch->declare_queue(
        queue      => $queue,
        on_success => sub {$done->send(1)},
        on_failure => sub {$done->send()},
    );
    die 'Declare queue failure' if !$done->recv;

    return;
}

sub delete_queue {
    my ($ch, $queue,) = @_;

    my $done = AnyEvent->condvar;
    $ch->delete_queue(
        queue      => $queue,
        on_success => sub {$done->send(1)},
        on_failure => sub {$done->send()},
    );
    die 'Delete queue failure' if !$done->recv;

    return;
}

sub consume {
    my ($ch, $queue, $handle_consume,) = @_;

    my $done = AnyEvent->condvar;
    $ch->consume(
        queue      => $queue,
        on_success => sub {$done->send(1)},
        on_failure => sub {$done->send()},
        on_consume => $handle_consume,
    );
    die 'Consume failure' if !$done->recv;

    return;
}

sub publish {
    my ($ch, $queue, $message,) = @_;

    $ch->publish(
        routing_key => $queue,
        body        => $message,
        mandatory   => 1,
        on_return   => sub {die 'Receive return'},
    );

    return;
}

sub handle_close {
    my $method_frame = shift->method_frame;
    die $method_frame->reply_code, $method_frame->reply_text;
}

