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
plan tests => 2;

use AnyEvent::RabbitMQ;

subtest 'No channels', sub {
    my $ar = connect_ar();
    ok $ar->is_open, 'connection is open';
    is channel_count($ar), 0, 'no channels open';

    close_ar($ar);
    ok !$ar->is_open, 'connection closed';
    is channel_count($ar), 0, 'no channels open';
};

subtest 'channels', sub {
    my $ar = connect_ar();
    ok $ar->is_open, 'connection is open';
    is channel_count($ar), 0, 'no channels open';

    my $ch = open_channel($ar);
    ok $ch->is_open, 'channel is open';
    is channel_count($ar), 1, 'no channels open';

    close_ar($ar);
    ok !$ar->is_open, 'connection closed';
    is channel_count($ar), 0, 'no channels open';
    ok !$ch->is_open, 'channel closed';
};

sub connect_ar {
    my $done = AnyEvent->condvar;
    my $ar = AnyEvent::RabbitMQ->new()->load_xml_spec()->connect(
        (map {$_ => $conf{$_}} qw(host port user pass vhost)),
        timeout    => 1,
        on_success => sub {$done->send(1)},
        on_failure => sub { diag @_; $done->send()},
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
        on_failure => sub { diag @_; $done->send()},
    );
    die 'Close failure' if !$done->recv;

    return;
}

sub channel_count {
    my ($ar,) = @_;

    return scalar keys %{$ar->channels};
}

sub open_channel {
    my ($ar,) = @_;
    
    my $done = AnyEvent->condvar;
    $ar->open_channel(
        on_success => sub {$done->send(shift)},
        on_failure => sub {$done->send()},
        on_return  => sub {die 'Receive return'},
        on_close   => \&handle_close,
    );
    my $ch = $done->recv;
    die 'Open channel failure' if !$ch;

    return $ch;
}

sub handle_close {
    my $method_frame = shift->method_frame;
    die $method_frame->reply_code, $method_frame->reply_text
      if $method_frame->reply_code;
}

