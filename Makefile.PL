use inc::Module::Install;

name 'AnyEvent-RabbitMQ';
all_from 'lib/AnyEvent/RabbitMQ.pm';

requires 'List::MoreUtils';
requires 'Net::AMQP';
requires 'AnyEvent';
requires 'Devel::GlobalDestruction';
requires 'namespace::clean';
requires 'File::ShareDir';
requires 'Readonly' => '1.03';

tests 't/*.t';
author_tests 'xt';
install_share;

build_requires 'Test::More';
build_requires 'Test::Exception';
build_requires 'version';
auto_install;
WriteAll;
