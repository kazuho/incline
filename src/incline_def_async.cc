#include "incline_def_async.h"

using namespace std;

string
incline_def_async::direct_expr(const string& col_expr) const
{
  string r;
  for (string::const_iterator ei = direct_expr_base_.begin();
       ei != direct_expr_base_.end();
       ++ei) {
    if (*ei == '?') {
      if (ei + 1 != direct_expr_base_.end() && *(ei + 1) == '?') {
	r += '?';
	++ei;
      } else {
	r += col_expr;
      }
    } else {
      r += *ei;
    }
  }
  return r;   
}
