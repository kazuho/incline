#include <cassert>
#include <cstdarg>
#include "incline_util.h"

#ifdef _MSC_VER
#  define SNPRINTF _snprintf_s
#  pragma warning(push)
#  pragma warning(disable : 4244) /* conversion from int to char */
#else
#  define SNPRINTF snprintf
#endif

using namespace std;

incline_util::filter_func_t::~filter_func_t()
{
}

string
incline_util::rewrite_prefix::operator()(const string& s) const
{
  return s.substr(0, orig_.size()) == orig_
    ? repl_ + s.substr(orig_.size()) : s;
}

vector<string>
incline_util::filter(const filter_func_t& func, const vector<string>& list)
{
  vector<string> r;
  for (vector<string>::const_iterator li = list.begin();
       li != list.end();
       ++li) {
    r.push_back(func(*li));
  }
  return r;
}

map<string, string>
incline_util::filter(const filter_func_t& keyfunc,
		     const filter_func_t& valuefunc,
		     const map<string, string>& map)
{
  std::map<string, string> r;
  for (std::map<string, string>::const_iterator mi = map.begin();
       mi != map.end();
       ++mi) {
    r[keyfunc(mi->first)] = valuefunc(mi->second);
  }
  return r;
}

string
incline_util::filter(const char* fmt, int idx, size_t n, ...)
{
  vector<const char*> repl;
  va_list args;
  va_start(args, n);
  for (size_t i = 0; i < n; i++) {
    repl.push_back(va_arg(args, const char*));
  }
  va_end(args);
  
  string r;
  for (const char* fi = fmt; *fi != '\0'; fi++) {
    if (*fi == '%') {
      ++fi;
      assert(*fi != '\0');
      if ('1' <= *fi && *fi <= '9') {
	assert((size_t)(*fi - '1') < repl.size());
	r += repl[(size_t)(*fi - '1')];
      } else if (*fi == 'I') {
	char buf[16];
	SNPRINTF(buf, sizeof(buf), "%d", idx);
	r += buf;
      } else {
	r.push_back(*fi);
      }
    } else {
      r.push_back(*fi);
    }
  }
  return r;
}

vector<string>
incline_util::filter(const char* fmt, const vector<string>& list)
{
  vector<string> r;
  int idx = 1;
  for (vector<string>::const_iterator li = list.begin();
       li != list.end();
       ++li, ++idx) {
    r.push_back(filter(fmt, idx, 1, li->c_str()));
  }
  return r;
}

vector<string>
incline_util::filter(const char* fmt, const map<string, string>& map)
{
  vector<string> r;
  int idx = 1;
  for (std::map<string, string>::const_iterator mi = map.begin();
       mi != map.end();
       ++mi, ++idx) {
    r.push_back(filter(fmt, idx, 2, mi->first.c_str(), mi->second.c_str()));
  }
  return r;
}
