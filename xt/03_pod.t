# XXX should be replaced by Dist::Zilla::Plugin::PodSyntaxTests, but holding
# of for eas of development on Ubuntu 18.04.

use Test::More;
eval "use Test::Pod 1.00";
plan skip_all => "Test::Pod 1.00 required for testing POD" if $@;
all_pod_files_ok();
