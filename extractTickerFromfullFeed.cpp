#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include<unordered_set>
using namespace std;

int main () {
  string row;
  ifstream myfile ("nasdaqFullFeed.csv");
  ofstream spy("spy.csv");
  ofstream uso("uso.csv");
  ofstream tlt("tlt.csv");
  ofstream vxx("vxx.csv");

  unordered_set<char> st;
  if (myfile.is_open())
  {
    while (getline(myfile,row))
    {
		st.insert(row[0]);
		if(row[0]=='A' || row[0]=='F' || row[0]=='E' || row[0]=='C' || row[0]=='X' || row[0]=='D' || row[0]=='U')
		{
			if(row.find(",USO,")!=string::npos ||row.find(",uso,")!=string::npos) 
			{
				uso << row << endl;
			}
			else if(row.find(",7030,")!=string::npos || row.find(",spy,")!=string::npos)
			{
				spy << row << endl;
			}
			else if(row.find(",TLT,")!=string::npos || row.find(",tlt,")!=string::npos)
			{
				tlt << row << endl;
			}
			else if(row.find(",VXX,")!=string::npos || row.find(",vxx,")!=string::npos)
			{
				vxx << row << endl;
			}
		}
		
	}	
  }

  for(unordered_set<char>::iterator it = st.begin();it!=st.end();++it)
  {
	  cout << *it << endl;
  } 
  
  myfile.close();
  spy.close();
  uso.close();
  tlt.close();
  vxx.close();

  return 0;
}


