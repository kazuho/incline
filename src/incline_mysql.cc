#include <sstream>
#include "incline_mysql.h"
#include "tmd.h"

using namespace std;

getoptpp::opt_str incline_mysql::opt_mysql_host_(0, "mysql-host", false,
						 "mysql host", "127.0.0.1");
getoptpp::opt_str incline_mysql::opt_mysql_user_(0, "mysql-user", false,
						 "mysql user", "root");
getoptpp::opt_str incline_mysql::opt_mysql_password_(0, "mysql-password", false,
						     "mysql password", "");
getoptpp::opt_int incline_mysql::opt_mysql_port_(0, "mysql-port", false,
						 "mysql port", 3306);


incline_mysql*
incline_mysql::factory::create(const string& host, unsigned short port)
{
  tmd::conn_t* dbh
    = new tmd::conn_t(host.empty() ? *opt_mysql_host_ : host, *opt_mysql_user_,
		      *opt_mysql_password_, *opt_database_,
		      port == 0 ? *opt_mysql_port_ : port);
  return new incline_mysql(dbh);
}

string
incline_mysql::factory::get_hostport() const
{
  stringstream ss;
  ss << *incline_mysql::opt_mysql_host_ << ':'
     << *incline_mysql::opt_mysql_port_;
  return ss.str();
}

incline_mysql::~incline_mysql()
{
  delete dbh_;
}

string
incline_mysql::escape(const string& s)
{
  return tmd::escape(*dbh_, s);
}

void
incline_mysql::execute(const string& stmt)
{
  try {
    tmd::execute(*dbh_, stmt);
  } catch (tmd::error_t& e) {
    switch (e.mysql_errno()) {
    case ER_LOCK_DEADLOCK:
      throw deadlock_error_t(e.what());
    case ER_LOCK_WAIT_TIMEOUT:
      throw timeout_error_t(e.what());
    default:
      throw;
    }
  }
}

void
incline_mysql::query(vector<vector<value_t> >& rows, const string& stmt)
{
  rows.clear();
  for (tmd::query_t res(*dbh_, stmt);
       ! res.fetch().eof();
       ) {
    rows.push_back(vector<value_t>());
    for (size_t i = 0; i < res.num_fields(); ++i) {
      rows.back().push_back(res.field(i));
    }
  }
}
