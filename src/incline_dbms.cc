#include <cstdarg>
#include <sstream>
#include "incline_config.h"
#include "incline_dbms.h"
#include "incline_util.h"
#ifdef WITH_MYSQL
# include "incline_mysql.h"
#endif
#ifdef WITH_PGSQL
# include "incline_pgsql.h"
#endif

using namespace std;

getoptpp::opt_str incline_dbms::opt_rdbms_('r', "rdbms", true, "rdbms name",
					   "");
getoptpp::opt_str incline_dbms::opt_database_('d', "database", true,
					      "database name");
getoptpp::opt_str incline_dbms::opt_host_('h', "host", false,
					   "database host", "127.0.0.1");
getoptpp::opt_int incline_dbms::opt_port_('P', "port", false, "database port",
					   0);
getoptpp::opt_str incline_dbms::opt_user_('u', "user", false, "database user",
					   "root");
getoptpp::opt_str incline_dbms::opt_password_('p', "password", false,
					       "database password", "");
incline_dbms::factory* incline_dbms::factory_ = NULL;

pair<string, unsigned short>
incline_dbms::factory::get_hostport() const
{
  unsigned short port = *incline_dbms::opt_port_;
  if (port == 0) {
    port = default_port();
  }
  return make_pair(*incline_dbms::opt_host_, port);
}

void
incline_dbms::query(std::vector<std::vector<value_t> >& rows, const char* fmt,
		    ...)
{
  char sql[10240];
  va_list args;
  va_start(args, fmt);
  vsprintf(sql, fmt, args);
  query(rows, string(sql));
}

bool
incline_dbms::setup_factory()
{
#ifdef WITH_MYSQL
  if (*opt_rdbms_ == "mysql" || *opt_rdbms_ == "mysqld") {
    factory_ = new incline_mysql::factory();
    return true;
  }
#endif
#ifdef WITH_PGSQL
  if (*opt_rdbms_ == "pgsql" || *opt_rdbms_ == "postgresql") {
    factory_ = new incline_pgsql::factory();
    return true;
  }
#endif
  return false;
}
