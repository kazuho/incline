#include "incline_driver.h"
#include "incline_def.h"

using namespace std;

incline_def*
incline_driver::create_def() const
{
  return new incline_def();
}
