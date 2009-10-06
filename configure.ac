AC_INIT(src/incline_def.cc)
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(src/incline_config.h)
AM_INIT_AUTOMAKE("incline", 0.02)

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_CPP
AC_PROG_GCC_TRADITIONAL
AC_PROG_LIBTOOL
LIBTOOL="$LIBTOOL --preserve-dup-deps"
AC_SUBST(LIBTOOL)

AC_ARG_WITH(mysql,
	[  --with-mysql=dir        mysql directory [[automatic]]],
	[MYSQL_DIR="$withval"])
if test "$MYSQL_DIR" = "yes" -o -z "$MYSQL_DIR"
then
	AC_CHECK_HEADERS([mysql.h], [FOUND_MYSQL=1])
	if test "$FOUND_MYSQL"
	then
		LDFLAGS="$LDFLAGS -lmysqlclient_r"
		WITH_MYSQL=1
		MYSQL_DIR="no"
	elif test -d /usr/local/mysql
	then
		MYSQL_DIR=/usr/local/mysql
	elif test "$MYSQL_DIR" = "yes"
	then
		AC_MSG_ERROR([could not find mysql])
	else
		MYSQL_DIR="no"
	fi
fi
if test "$MYSQL_DIR" != "no"
then
	if test ! -d "$MYSQL_DIR"
	then
		AC_MSG_ERROR([could not find mysql at given location])
	fi
	CPPFLAGS="$CPPFLAGS -I$MYSQL_DIR/include/mysql -I$MYSQL_DIR/include"
	LDFLAGS="$LDFLAGS -L$MYSQL_DIR/lib/mysql -L$MYSQL_DIR/lib -lmysqlclient_r"
	WITH_MYSQL=1
fi
if test "$WITH_MYSQL"
then
	AC_DEFINE([WITH_MYSQL],1,[create mysql driver])
fi
AM_CONDITIONAL(WITH_MYSQL, test "$WITH_MYSQL")

AC_ARG_WITH(pgsql,
	[  --with-pgsql=dir        postgresql directory [[automatic]]],
	[PGSQL_DIR="$withval"])
if test "$PGSQL_DIR" = "yes" -o -z "$PGSQL_DIR"
then
	AC_CHECK_HEADERS([libpq-fe.h], [FOUND_PGSQL=1])
	if test "$FOUND_PGSQL"
	then
		LDFLAGS="$LDFLAGS -lpq"
		WITH_PGSQL=1
		PGSQL_DIR="no"
	elif test -d /usr/local/pgsql
	then
		PGSQL_DIR=/usr/local/pgsql
	elif test "$PGSQL_DIR" = "yes"
	then
		AC_MSG_ERROR([could not find pgsql])
	else
		PGSQL_DIR="no"
	fi
fi
if test "$PGSQL_DIR" != "no"
then
	if test ! -d "$PGSQL_DIR"
	then
		AC_MSG_ERROR([could not find pgsql at given location])
	fi
	CPPFLAGS="$CPPFLAGS -I$PGSQL_DIR/include"
	LDFLAGS="$LDFLAGS -L$PGSQL_DIR/lib -lpq"
	WITH_PGSQL=1
fi
if test "$WITH_PGSQL"
then
	AC_DEFINE([WITH_PGSQL],1,[create pgsql driver])
fi
AM_CONDITIONAL(WITH_PGSQL, test "$WITH_PGSQL")

if test "$WITH_MYSQL" = "" -a "$WITH_PGSQL" = ""
then
	AC_MSG_ERROR([no datatabase driver found, please set them manually using --with-XXX])
fi

CFLAGS="$CFLAGS -Wall -g -O"
CXXFLAGS="$CXXFLAGS -Wall -g -O"

AC_OUTPUT(Makefile src/Makefile)
