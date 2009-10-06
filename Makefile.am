SUBDIRS=	src
man_MANS=	incline.1
EXTRA_DIST=	README.pod README README.html $(man_MANS) META.yml.pl META.yml example/*.json t/*.t

incline.1: README.pod
	pod2man --name=incline --section=1 < README.pod > $@

README: README.pod
	pod2text < README.pod > $@

README.html: README.pod
	pod2html --title="Incline - a replicator for RDB shards" < README.pod > $@

META.yml: META.yml.pl
	perl $< > $@

test:
	perl -MTest::Harness -we 'runtests(@ARGV)' t/*.t
