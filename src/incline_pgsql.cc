#include <sstream>
#include "incline_util.h"
#include "incline_pgsql.h"

using namespace std;

#define THROW_PQ_ERROR(dbh) throw error_t(PQerrorMessage(dbh))

class PGresultWrap {
protected:
  PGresult* r_;
public:
  PGresultWrap(PGresult* r) : r_(r) {}
  ~PGresultWrap() { PQclear(r_); }
  PGresult* operator*() { return r_; }
private:
  PGresultWrap(const PGresultWrap&); // noncopyable
  PGresultWrap& operator=(const PGresultWrap&);
};

static string escape_quote(const string& src)
{
  string r;
  string::size_type start = 0, quote_at;
  while ((quote_at = src.find('\'', start)) != string::npos) {
    r += src.substr(start, quote_at - start);
    r += "''";
    start = quote_at + 1;
  }
  r += src.substr(start);
  return r;
}

incline_pgsql*
incline_pgsql::factory::create(const string& host, unsigned short port,
			       const string& user, const string& password)
{
  if (port == 0) {
    port = *opt_port_ == 0 ? default_port() : *opt_port_;
  }
  return new incline_pgsql(host.empty() ? *opt_host_ : host, port,
			   user.empty() ? *opt_user_ : user,
			   password.empty() ? *opt_password_ : password);
}

vector<string>
incline_pgsql::factory::create_trigger(const string& name, const string& event,
				       const string& time, const string& table,
				       const string& funcbody) const
{
  vector<string> r;
  r.push_back("CREATE FUNCTION " + name + "() RETURNS TRIGGER AS 'BEGIN\n"
	      + escape_quote(funcbody) + "  RETURN NEW;\nEND"
	      + "' language 'plpgsql'");
  r.push_back("CREATE TRIGGER " + name + ' ' + time + ' ' + event
	      + " ON " + table + " FOR EACH ROW EXECUTE PROCEDURE " + name
	      + "()");
  return r;
}

vector<string>
incline_pgsql::factory::drop_trigger(const string& name, const string& table,
				     bool if_exists) const
{
  vector<string> r;
  r.push_back(string("DROP TRIGGER ") + (if_exists ? "IF EXISTS " : "") + name
	      + " ON " + table);
  r.push_back(string("DROP FUNCTION ") + (if_exists ? "IF EXISTS " : "") + name
	      + "()");
  return r;
}

string
incline_pgsql::factory::create_queue_table(const string& table_name,
					   const string& column_defs,
					   bool if_not_exists) const
{
  return string("CREATE TABLE ") + (if_not_exists ? "IF NOT EXISTS " : "")
    + table_name + " (_iq_id SERIAL,_iq_action CHAR(1) NOT NULL," + column_defs
    + ",PRIMARY KEY (_iq_id))";
}

string
incline_pgsql::factory::delete_using(const string& table_name,
				     const vector<string>& using_list) const
{
  return "DELETE FROM " + table_name + " USING "
    + incline_util::join(',', using_list);
}

incline_pgsql::~incline_pgsql()
{
  PQfinish(dbh_);
}

string
incline_pgsql::escape(const string& s)
{
  char* buf = (char*)alloca(s.size() * 2 + 1);
  int err = 0;
  PQescapeStringConn(dbh_, buf, s.c_str(), s.size(), &err);
  if (err != 0) {
    THROW_PQ_ERROR(dbh_);
  }
  return string(buf);
}

void
incline_pgsql::execute(const string& stmt)
{
  PGresultWrap ret(PQexec(dbh_, stmt.c_str()));
  assert(*ret != NULL);
  switch (PQresultStatus(*ret)) {
  case PGRES_COMMAND_OK:
    break;
  case PGRES_EMPTY_QUERY:
  case PGRES_COPY_OUT:
  case PGRES_COPY_IN:
  case PGRES_TUPLES_OK:
    throw error_t("unexpected response from pgsql server");
  default:
    THROW_PQ_ERROR(dbh_);
  }
}

void
incline_pgsql::query(vector<vector<value_t> >& rows, const string& stmt)
{
  rows.clear();
  // send query, check response type
  PGresultWrap ret(PQexec(dbh_, stmt.c_str()));
  assert(*ret != NULL);
  switch (PQresultStatus(*ret)) {
  case PGRES_TUPLES_OK:
    break;
  case PGRES_EMPTY_QUERY:
  case PGRES_COMMAND_OK:
  case PGRES_COPY_OUT:
  case PGRES_COPY_IN:
    throw error_t("unexpected response from pgsql server");
  default:
    THROW_PQ_ERROR(dbh_);
  }
  // read response
  int ntuples = PQntuples(*ret),
    nfields = PQnfields(*ret);
  for (int i = 0; i < ntuples; ++i) {
    rows.push_back(vector<value_t>());
    for (int j = 0; j < nfields; ++j) {
      rows.back().push_back(PQgetvalue(*ret, i, j));
    }
  }
}

string
incline_pgsql::get_column_def(const string& table_name,
			      const string& column_name)
{
  vector<vector<value_t> > rows;
  super::query(rows,
	       "SELECT DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE UDT_CATALOG='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
	       escape(*incline_dbms::opt_database_).c_str(),
	       escape(table_name).c_str(),
	       escape(column_name).c_str());
  if (rows.size() != 1) {
    throw error_t("failed to obtain column definition of column:" + table_name
		  + '.' + column_name);
  }
  string def(*rows[0][0]);
  if (! rows[0][1]->empty()) {
    def += '(' + *rows[0][1] + ')';
  }
  if (*rows[0][1] == "NO") {
    def += " NOT NULL";
  }
  return def;
}

incline_pgsql::incline_pgsql(const string& host, unsigned short port,
			     const string& user, const string& password)
  : super(host, port), dbh_(NULL)
{
  stringstream conninfo;
  conninfo << "host=" << host_ << " port=" << port_ << " dbname="
	   << *opt_database_ << " user=" << user << " password=" << password;
  dbh_ = PQconnectdb(conninfo.str().c_str());
  assert(dbh_ != NULL);
  if (PQstatus(dbh_) != CONNECTION_OK) {
    THROW_PQ_ERROR(dbh_);
  }
}
