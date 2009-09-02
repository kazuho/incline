#include "incline_mysql.h"
#include "incline_util.h"
#include "tmd.h"

using namespace std;

incline_mysql*
incline_mysql::factory::create(const string& host, unsigned short port,
			       const string& user, const string& password)
{
  if (port == 0) {
    port = *opt_port_ == 0 ? default_port() : *opt_port_;
  }
  return new incline_mysql(host.empty() ? *opt_host_ : host, port,
			   user.empty() ? *opt_user_ : user,
			   password.empty() ? *opt_password_ : password);
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

vector<string>
incline_mysql::factory::drop_trigger(const string& name, const string& table,
				    bool if_exists) const
{
  return
    incline_util::vectorize(string("DROP TRIGGER ")
			    + (if_exists ? "IF EXISTS " : "") + name);
}

string
incline_mysql::factory::create_queue_table(const string& table_name,
					   const string& column_defs,
					   bool if_not_exists) const
{
  return string("CREATE TABLE ") + (if_not_exists ? "IF NOT EXISTS " : "")
    + table_name + " (_iq_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
    "_iq_action CHAR(1) CHARACTER SET latin1 NOT NULL," + column_defs
    + ",PRIMARY KEY (_iq_id))";
}

string
incline_mysql::factory::delete_using(const string& table_name,
				     const vector<string>& using_list) const
{
  string sql = "DELETE FROM " + table_name + " USING " + table_name;
  for (vector<string>::const_iterator ui = using_list.begin();
       ui != using_list.end();
       ++ui) {
    sql += " INNER JOIN " + *ui;
  }
  return sql;
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

string
incline_mysql::get_column_def(const string& table_name,
			      const string& column_name)
{
  tmd::query_t res(*dbh_,
		   "SELECT UPPER(COLUMN_TYPE),CHARACTER_SET_NAME,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
		   tmd::escape(*dbh_, *incline_dbms::opt_database_).c_str(),
		   tmd::escape(*dbh_, table_name).c_str(),
		   tmd::escape(*dbh_, column_name).c_str());
  if (res.fetch().eof()) {
    throw error_t("failed to obtain column definition of column:" + table_name
		  + '.' + column_name);
  }
  string def(res.field(0));
  if (res.field(1) != NULL) {
    def += string(" CHARSET ") + res.field(1);
  }
  if (strcmp(res.field(2), "NO") == 0) {
    def += " NOT NULL";
  }
  return def;
}

incline_mysql::incline_mysql(const string& host, unsigned short port,
			     const string& user, const string& password)
  : super(host, port), dbh_(NULL)
{
  dbh_ = new tmd::conn_t(host_, user, password, *opt_database_, port_);
}
