/* Copyright 2008,2009 Cybozu Labs, Inc.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY CYBOZU LABS, INC. ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL CYBOZU LABS, INC. OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Cybozu Labs, Inc.
 *
 */

#ifndef tmd_H
#define tmd_H

extern "C" {
#include <assert.h>
#include <stdarg.h>
#include <mysql.h>
#include <mysqld_error.h>
}
#include <stdexcept>
#include <string>
#include <vector>

namespace tmd {
  
  std::string ssprintf(const char* fmt, ...) __attribute__((__format__(__printf__, 1, 2)));
  inline std::string ssprintf(const char* fmt, ...)
  {
    char buf[4096];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    return buf;
  }    
  
  class error_t : public std::domain_error {
  protected:
    unsigned int mysql_errno_;
  public:
    error_t(const std::string& s, unsigned int mysql_errno = 0)
      : std::domain_error(s), mysql_errno_(mysql_errno) {
      fprintf(stderr, "%d:%s\n", mysql_errno, s.c_str());
    }
    unsigned int mysql_errno() const { return mysql_errno_; }
  };
  
  inline std::string getenvif(const std::string& name, const std::string& defval)
  {
    const char* s;
    if ((s = getenv(name.c_str())) != NULL) {
      return s;
    }
    return defval;
  }
  
  inline int getenvif(const std::string& name, int defval)
  {
    const char* s;
    if ((s = getenv(name.c_str())) != NULL) {
      int v;
      if (sscanf(s, "%d", &v) != 1) {
	throw error_t(ssprintf("failed to parse environment variable %s=%s as a number\n.",
			       name.c_str(), s));
      }
      return v;
    }
    return defval;
  }
  
  inline std::string getline(FILE* fp)
  {
    char buf[16384];
    if (fgets(buf, sizeof(buf), fp) == NULL) {
      return std::string();
    }
    size_t l = strlen(buf);
    if (l != 0 && buf[l - 1] == '\n') {
      buf[--l] = '\0';
    }
    return buf;
  }
  
  inline std::vector<std::string> split(const char* str, int delimiter, size_t n)
  {
    std::vector<std::string> arr;
    while (arr.size() + 1 < n) {
      const char* d = strchr(str, delimiter);
      if (d == NULL) {
	break;
      }
      arr.push_back(std::string(str, d));
      str = d + 1;
    }
    arr.push_back(str);
    return arr;
  }
  
  template<typename It> std::string join(const It& first, const It& last, const std::string& delimiter)
  {
    std::string out;
    for (It i = first; i != last; ++i) {
      if (i != first) {
	out += delimiter;
      }
      out += *i;
    }
    return out;
  }
  
#define TMD_STR2X(suffix, type)						\
  inline unsigned str2##suffix(const char *s) {				\
    type v;								\
    int r = sscanf(s, "%" #suffix, &v);					\
    assert(r == 1);							\
    return v;								\
  }
  TMD_STR2X(u, unsigned)
  TMD_STR2X(d, int)
#undef TMD_STR2X
  
#define TMD_X2STR(suffix, type)		\
  inline std::string suffix##2str(type v) {	\
    char buf[32];			\
    sprintf(buf, "%" #suffix, v);	\
    return buf;				\
  }
  TMD_X2STR(u, unsigned)
  TMD_X2STR(d, int)
#undef TMD_X2STR

  inline std::string escape(MYSQL* mysql, const std::string& s)
  {
    char* buf = new char[s.size() * 2 + 1];
    mysql_real_escape_string(mysql, buf, s.c_str(), s.size());
    std::string r(buf);
    delete [] buf;
    return r;
  }
  
  inline void _execute(MYSQL* mysql, const char* sql)
  {
    int ret = mysql_query(mysql, sql);
    if (ret != 0) {
      throw error_t(ssprintf("mysql error:%s for sql: %s\n", mysql_error(mysql),
			     sql),
		    mysql_errno(mysql));
    }
  }
  
  inline void execute(MYSQL* mysql, const std::string& sql)
  {
    _execute(mysql, sql.c_str());
  }

  inline void vexecute(MYSQL* mysql, const char* fmt, va_list args)
  {
    char sql[10240];
    vsprintf(sql, fmt, args);
    _execute(mysql, sql);
  }
  
  void execute(MYSQL* mysql, const char* fmt, ...)
    __attribute__((__format__(__printf__, 2, 3)));
  inline void execute(MYSQL* mysql, const char* fmt, ...)
  {
    va_list args;
    va_start(args, fmt);
    vexecute(mysql, fmt, args);
    va_end(args);
  }
  
  inline unsigned long long affected_rows(MYSQL* mysql)
  {
    return mysql_affected_rows(mysql);
  }
  
  class query_t {
  protected:
    MYSQL_RES* res_;
    MYSQL_ROW row_;
    unsigned long* lengths_;
    unsigned int num_fields_;
  public:
    query_t(MYSQL* mysql, const std::string& sql)
      : row_(NULL), lengths_(NULL), num_fields_(0) {
      execute(mysql, sql);
      res_ = mysql_use_result(mysql);
      assert(res_ != NULL);
      num_fields_ = mysql_num_fields(res_);
    }
    query_t(MYSQL* mysql, const char* fmt, ...)
      __attribute__((__format__(__printf__, 3, 4)))
      : row_(NULL), lengths_(NULL) {
      va_list args;
      va_start(args, fmt);
      vexecute(mysql, fmt, args);
      va_end(args);
      res_ = mysql_store_result(mysql);
      assert(res_ != NULL);
      num_fields_ = mysql_num_fields(res_);
    }
    ~query_t() { mysql_free_result(res_); }
    query_t& fetch() {
      row_ = mysql_fetch_row(res_);
      lengths_ = mysql_fetch_lengths(res_);
      return *this;
    }
    size_t num_fields() const { return num_fields_; }
    bool eof() const { return row_ == NULL; }
    const char* field(size_t idx) const { return row_[idx]; }
    unsigned long field_length(size_t idx) const { return lengths_[idx]; }
  private:
    query_t(const query_t&);
    query_t& operator=(const query_t&);
  public:
  };
  
  class conn_t {
  protected:
    MYSQL* conn_;
    std::string host_;
    std::string user_;
    std::string password_;
    std::string database_;
    unsigned short port_;
  public:
    conn_t(const std::string& host, const std::string& user, const std::string& password,
	      const std::string& database, unsigned short port)
      : conn_(NULL), host_(host), user_(user), password_(password),
	database_(database), port_(port)
    {}
    ~conn_t() {
      if (conn_ != NULL) {
	mysql_close(conn_);
	conn_ = NULL;
      }
    }
    operator MYSQL*() {
      if (conn_ == NULL) {
	conn_ = mysql_init(NULL);
	assert(conn_ != NULL);
	if (mysql_real_connect(conn_, host_.c_str(), user_.c_str(),
			       password_.c_str(), database_.c_str(), port_,
			       NULL, 0)
	    == NULL) {
	  throw error_t(ssprintf("failed to connect to mysql:%s:user=%s;host=%s;port=%hu\n",
				 database_.c_str(),
				 user_.c_str(),
				 host_.c_str(),
				 port_),
			mysql_errno(conn_));
	}
      }
      return conn_;
    }
  private:
    conn_t(const conn_t&);
    conn_t& operator=(const conn_t&);
  };

}

#endif
