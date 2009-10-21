#include <cassert>
#include <cstdarg>
#include "incline_util.h"

using namespace std;

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
	sprintf(buf, "%d", idx);
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
