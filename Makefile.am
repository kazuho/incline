SUBDIRS=	src
man_MANS=	incline.1
EXTRA_DIST=	$(man_MANS) Makefile.PL META.yml META.yml.pl lib/App/Incline.pm README README.html example/*.json t/*.pl t/*.pm t/*.t benchmark/*.json benchmark/*.pl

incline.1: lib/App/Incline.pm
	pod2man --name=incline --section=1 < lib/App/Incline.pm > $@

README: lib/App/Incline.pm
	pod2text < lib/App/Incline.pm > $@

README.html: lib/App/Incline.pm
	pod2html --title="Incline - a replicator for RDB shards" < lib/App/Incline.pm > $@

META.yml: META.yml.pl src/incline_config.h
	perl $< > $@

dist-hook:
	(cd $(distdir) && find * -type f) > $(distdir)/MANIFEST

test:
	perl -MTest::Harness -we 'runtests(@ARGV)' t/*.t
