BEGIN { unshift @INC, '.' }

use inc::Module::Install;

name 'AnyEvent-RabbitMQ';
all_from 'lib/AnyEvent/RabbitMQ.pm';

requires 'List::MoreUtils';
requires 'Net::AMQP' => '0.06';
requires 'AnyEvent';
requires 'Devel::GlobalDestruction';
requires 'namespace::clean';
requires 'File::ShareDir';
requires 'Readonly' => '1.03';

tests 't/*.t';
author_tests 'xt';
install_share;

perl_version '5.10';
build_requires 'Test::More';
build_requires 'Test::Exception';
build_requires 'version';

resources(
    repository => 'git://github.com/bobtfish/AnyEvent-RabbitMQ.git',
);

contributors 'Tom Doran <bobtfish@cpan.org>';
contributors 'Nicolas R <atoomic@cpan.org>';
contributors 'Dave Lambley <dlambley@cpan.org>';
contributors 'Ruslan Zakirov <ruz@bestpractical.com>'

WriteAll;

