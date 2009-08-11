AC_INIT(src/incline_def.cc)
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(src/incline_config.h)
AM_INIT_AUTOMAKE("incline", 0.1)

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
	[
MYSQL_DIR="$withval"
], [
if test -d /usr/local/mysql
then
	MYSQL_DIR=/usr/local/mysql
fi
])
if test -n "$MYSQL_DIR"
then
	if test ! -d "$MYSQL_DIR"
	then
		AC_MSG_ERROR(["failed to locate mysql"])
	fi
	CFLAGS="$CFLAGS -I$MYSQL_DIR/include/mysql -I$MYSQL_DIR/include"
	CXXFLAGS="$CXXFLAGS -I$MYSQL_DIR/include/mysql -I$MYSQL_DIR/include"
	LDFLAGS="$LDFLAGS -L$MYSQL_DIR/lib/mysql -L$MYSQL_DIR/lib"
fi

CFLAGS="$CFLAGS -Wall -g -O"
CXXFLAGS="$CXXFLAGS -Wall -g -O"
LDFLAGS="$LDFLAGS -lmysqlclient_r"

AC_OUTPUT(Makefile src/Makefile)
