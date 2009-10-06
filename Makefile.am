SUBDIRS=	src
man_MANS=	incline.1
EXTRA_DIST=	$(man_MANS) Makefile.PL META.yml META.yml.pl README.pod README README.html example/*.json t/*.pl t/*.pm t/*.t

incline.1: README.pod
	pod2man --name=incline --section=1 < README.pod > $@

README: README.pod
	pod2text < README.pod > $@

README.html: README.pod
	pod2html --title="Incline - a replicator for RDB shards" < README.pod > $@

META.yml: META.yml.pl
	perl $< > $@

dist-hook:
	(cd $(distdir) && find * -type f) > $(distdir)/MANIFEST

test:
	perl -MTest::Harness -we 'runtests(@ARGV)' t/*.t
