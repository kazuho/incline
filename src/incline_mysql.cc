#include "incline_mysql.h"
#include "incline_util.h"
#include "tmd.h"

using namespace std;

incline_mysql*
incline_mysql::factory::create(const string& host, unsigned short port)
{
  if (port == 0) {
    port = *opt_port_ == 0 ? default_port() : *opt_port_;
  }
  return new incline_mysql(host.empty() ? *opt_host_ : host, port);
}

vector<string>
incline_mysql::factory::create_trigger(const string& name, const string& event,
				       const string& time, const string& table,
				       const string& funcbody) const
{
  string r = "CREATE TRIGGER " + name + ' ' + time + ' ' + event + " ON "
    + table + " FOR EACH ROW BEGIN\n" + funcbody + "END";
  return incline_util::vectorize(r);
}

incline_mysql::~incline_mysql()
{
  delete dbh_;
}

string
incline_mysql::escape(const string& s)
{
  try {
    return tmd::escape(*dbh_, s);
  } catch (tmd::error_t& e) {
    throw error_t(e.what());
  }
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
      throw error_t(e.what());
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

incline_mysql::incline_mysql(const string& host, unsigned short port)
  : super(host, port), dbh_(NULL)
{
  dbh_ = new tmd::conn_t(host_, *opt_user_, *opt_password_, *opt_database_,
			 port_);
}
