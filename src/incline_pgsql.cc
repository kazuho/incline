#include <sstream>
#include "incline_pgsql.h"
#include "tmd.h"

using namespace std;

#define THROW_PQ_ERROR(dbh) throw error_t(PQerrorMessage(dbh))

getoptpp::opt_str incline_pgsql::opt_pgsql_host_(0, "pgsql-host", false,
						 "pgsql host", "127.0.0.1");
getoptpp::opt_str incline_pgsql::opt_pgsql_user_(0, "pgsql-user", false,
						 "pgsql user", "postgres");
getoptpp::opt_str incline_pgsql::opt_pgsql_password_(0, "pgsql-password", false,
						     "pgsql password", "");
getoptpp::opt_int incline_pgsql::opt_pgsql_port_(0, "pgsql-port", false,
						 "pgsql port", 3306);

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
  return new incline_pgsql(host.empty() ? *opt_pgsql_host_ : host,
			   port == 0 ? *opt_pgsql_port_ : port);
}

string
incline_pgsql::factory::get_hostport() const
{
  stringstream ss;
  ss << *incline_pgsql::opt_pgsql_host_ << ':'
     << *incline_pgsql::opt_pgsql_port_;
  return ss.str();
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
  conninfo << "host=" << (host.empty() ? *opt_pgsql_host_ : host) << " port="
	   << (port == 0 ? *opt_pgsql_port_ : port) << " dbname="
	   << *opt_database_ << " user=" << *opt_pgsql_user_ << " password="
	   << *opt_pgsql_password_;
  dbh_ = PQconnectdb(conninfo.str().c_str());
  assert(dbh_ != NULL);
  if (PQstatus(dbh_) != CONNECTION_OK) {
    THROW_PQ_ERROR(dbh_);
  }
}
