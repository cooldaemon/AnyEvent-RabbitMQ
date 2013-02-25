use Test::More;
eval q{ use Test::Spelling };
plan skip_all => "Test::Spelling is not installed." if $@;
add_stopwords(map { split /[\s\:\-]/ } <DATA>);
set_spell_cmd('aspell list');
$ENV{LANG} = 'C';
all_pod_files_spelling_ok('lib');
__DATA__
signalled
cancelled
Masahito Ikuta
cooldaemon@gmail.com
AMQP
RabbitMQ
multi
ack
qos
