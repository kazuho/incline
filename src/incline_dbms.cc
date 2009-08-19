#include "incline_mysql.h"

using namespace std;

getoptpp::opt_str incline_dbms::opt_rdbms_('r', "rdbms", false,
					   "rdbms name", "mysql");
getoptpp::opt_str incline_dbms::opt_database_('d', "database", true,
					      "database name");
incline_dbms::factory* incline_dbms::factory_ = NULL;

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
  if (*opt_rdbms_ == "mysql") {
    factory_ = new incline_mysql::factory();
  } else {
    return false;
  }
  return true;
}
