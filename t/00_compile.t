use strict;
use Test::More tests => 3;

BEGIN {
    use_ok 'AnyEvent::RabbitMQ';
    use_ok 'AnyEvent::RabbitMQ::Channel';
    use_ok 'AnyEvent::RabbitMQ::LocalQueue';
}
