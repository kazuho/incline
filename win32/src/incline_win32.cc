#include <windows.h>
#include "incline_win32.h"

void sleep(unsigned int seconds)
{
  // bare minimum :-p
  Sleep(seconds * 1000);
}
