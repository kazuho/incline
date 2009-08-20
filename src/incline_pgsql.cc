#include <sstream>
#include "incline_pgsql.h"
#include "tmd.h"

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

incline_pgsql*
incline_pgsql::factory::create(const string& host, unsigned short port)
{
  if (port == 0) {
    port = *opt_port_ == 0 ? default_port() : *opt_port_;
  }
  return new incline_pgsql(host.empty() ? *opt_host_ : host, port);
}

vector<string>
incline_pgsql::factory::create_trigger(const string& name, const string& event,
				       const string& time, const string& table,
				       const string& funcbody) const
{
  vector<string> r;
  r.push_back("CREATE FUNCTION " + name + "() RETURNS TRIGGER AS 'BEGIN\n"
	      + funcbody + "RETURN NEW;\nEND" + "' language 'plpgsql'");
  r.push_back("CREATE TRIGGER " + name + ' ' + time + ' ' + event
	      + " ON " + table + " FOR EACH ROW EXECUTE PROCEDURE " + name
	      + "()");
  return r;
}

vector<string>
incline_pgsql::factory::drop_trigger(const string& name, bool if_exists) const
{
  vector<string> r(super::drop_trigger(name, if_exists));
  r.push_back(string("DROP FUNCTION ") + (if_exists ? "IF EXISTS " : "")
	      + name);
  return r;
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

incline_pgsql::incline_pgsql(const string& host, unsigned short port)
  : super(host, port), dbh_(NULL)
{
  stringstream conninfo;
  conninfo << "host=" << host_ << " port=" << port_ << " dbname="
	   << *opt_database_ << " user=" << *opt_user_ << " password="
	   << *opt_password_;
  dbh_ = PQconnectdb(conninfo.str().c_str());
  assert(dbh_ != NULL);
  if (PQstatus(dbh_) != CONNECTION_OK) {
    THROW_PQ_ERROR(dbh_);
  }
}
