#include<iostream>
#include<string>
#include<fstream>
#include<sstream>
#include<stdlib.h>
#include<queue>
#include<unordered_set>
#include<vector>
#include<unordered_map>
using namespace std;

class generalInfo{
	
	string code;
	int  stockLocate;
	int  trackingNumber;
	long timestamp;

	public:
		
		string getcode()  const{return code;}
		long gettimestamp() const{return timestamp;}
		int getStockLocate() const{return stockLocate;}
		int getTrackingNumber() const{return trackingNumber;}
	
		generalInfo(string code,int stockLocate,int trackingNumber,long timestamp):code(code),stockLocate(stockLocate),trackingNumber(trackingNumber),
				timestamp(timestamp){}
		
		//copy constructor
		generalInfo(const generalInfo& x):code(x.getcode()),stockLocate(x.getStockLocate()),trackingNumber(x.getTrackingNumber()),
			timestamp(x.gettimestamp()){}
		
		
		generalInfo(generalInfo&& x):code(x.getcode()),stockLocate(x.getStockLocate()),trackingNumber(x.getTrackingNumber()),
			timestamp(x.gettimestamp()){}
		
		generalInfo& operator= (generalInfo&& x){
			code = x.getcode();
			stockLocate = x.getStockLocate();
			trackingNumber = x.getTrackingNumber();
			timestamp = x.gettimestamp();
			return *this;
		}
		
		generalInfo& operator= (const generalInfo& x){
			code = x.getcode();
			stockLocate = x.getStockLocate();
			trackingNumber = x.getTrackingNumber();
			timestamp = x.gettimestamp();
			return *this;
		}

		
		~generalInfo(){}
	
};


class xclusiveInfo{
	string stock = "";
	string indicator = "";

	int		shares = 0;
	int     canceledShares = 0;
	double  price = 0.0;
	string  attribution = "";
	
	
	public:
	
	string getStock() const{return stock;}
	string getIndicator() const{return indicator;}
	int    getCanceledShares() const{return canceledShares;}
	int getShares() const{return shares;}
	double getPrice() const{return price;}
	string getAttribution() const{return attribution;}
	void updateShares(int nshares)
	{
		cout << " nshares "<< nshares << endl;
		this->shares = nshares; 
		cout << " this shares " << shares << endl;
		cout << " ..." << getShares() << endl;
	} 
	void setPrice(double nprice) {price = nprice;}
	void setShares(int nshares){ shares = nshares;}
	
	xclusiveInfo(string stock,string indicator,int shares,double price,string attribution,int canceledShares):stock(stock),indicator(indicator),shares(shares),
									price(price),attribution(attribution),canceledShares(canceledShares){}
	
	xclusiveInfo(const xclusiveInfo& x):stock(x.getStock()),indicator(x.getIndicator()),shares(x.getShares()),
		price(x.getPrice()),attribution(x.getAttribution()),canceledShares(x.getCanceledShares()){}
	xclusiveInfo(xclusiveInfo&& x):stock(x.getStock()),indicator(x.getIndicator()),shares(x.getShares()),price(x.getPrice())
	,attribution(x.getAttribution()),canceledShares(x.getCanceledShares()){}	
	
	xclusiveInfo& operator=(const xclusiveInfo& x)
	{
		stock = x.getStock();
		indicator = x.getIndicator();
		shares = x.getShares();
		price = x.getPrice();
		attribution = x.getAttribution();
		canceledShares = x.getCanceledShares();
		return *this;
	}

	xclusiveInfo& operator=(xclusiveInfo&& x)
	{
		stock = x.getStock();
		indicator = x.getIndicator();
		shares = x.getShares();
		price = x.getPrice();
		attribution = x.getAttribution();
		canceledShares = x.getCanceledShares();
		return *this;
	}
	
	~xclusiveInfo(){}
	
};


class order{
	generalInfo ginfo;
	long   orderReferenceNumber;
	long   oldOrderReferenceNumber;
	xclusiveInfo xinfo;
	
	public:

		generalInfo getGeneralInfo() const{return ginfo;}
		long getOrderReferenceNumber() const{return orderReferenceNumber;}
		long getOldOrderReferenceNumber() const{return oldOrderReferenceNumber;}
		xclusiveInfo getXclusiveInfo() const{return xinfo;}
		void setOrderReferenceNumber(const long& norderReferenceNumber) { orderReferenceNumber = norderReferenceNumber;}
		
		order(string code,int stockLocate,int trackingNumber,long timestamp,long orderReferenceNumber,long oldOrderReferenceNumber,string stock,
		string indicator,int shares,double price,string attribution,int canceledShares):
			ginfo{generalInfo(code,stockLocate,trackingNumber,timestamp)},orderReferenceNumber{orderReferenceNumber},
			xinfo{xclusiveInfo(stock,indicator,shares,price,attribution,canceledShares)},oldOrderReferenceNumber{oldOrderReferenceNumber}{}
		
		order(const order& x):ginfo(x.getGeneralInfo()),orderReferenceNumber(x.getOrderReferenceNumber()),xinfo(x.getXclusiveInfo()),
					oldOrderReferenceNumber{x.getOldOrderReferenceNumber()}{}
		order(order&& x):ginfo(x.getGeneralInfo()),orderReferenceNumber(x.getOrderReferenceNumber()),xinfo(x.getXclusiveInfo())
					,oldOrderReferenceNumber{x.getOldOrderReferenceNumber()}{}
		
		order& operator=(const order& x){
			ginfo = x.getGeneralInfo();
			orderReferenceNumber = x.getOrderReferenceNumber();
			oldOrderReferenceNumber = x.getOldOrderReferenceNumber();
			xinfo = x.getXclusiveInfo();
			return *this;
		}
		
		order& operator=(order&& x){
			ginfo = x.getGeneralInfo();
			orderReferenceNumber = x.getOrderReferenceNumber();
			oldOrderReferenceNumber = x.getOldOrderReferenceNumber();			
			xinfo = x.getXclusiveInfo();
			return *this;
		}
		
		void printOrder() const 
		{
			cout << ginfo.getcode() << "," << ginfo.getStockLocate() << ","<<ginfo.getTrackingNumber()<<","<<ginfo.gettimestamp()<<","<<
			orderReferenceNumber << "," << oldOrderReferenceNumber << "," << xinfo.getIndicator() << "," << xinfo.getShares() << "," << xinfo.getStock()
			<< "," << xinfo.getPrice()<< ", Executed/Canceled shaers: "<< xinfo.getCanceledShares()<< endl;
		}

		
		~order(){}
};

class orderFactoryClass{
	
	public:
		static order getOrderObj(string s)
		{
			stringstream ss(s);
			string token;
			string code;
			int stockLocate,trackingNumber;
			long timestamp,orderReferenceNumber,oldOrderReferenceNumber;
			
			string stock="";
			string indicator="";

			int		shares=0,canceledShares=0;
			double price= 0;
			string attribution = "";
			
			getline(ss,code,',');
			getline(ss,token,',');stockLocate = stoi(token);
			getline(ss,token,',');trackingNumber = stoi(token);
			getline(ss,token,',');timestamp = stol(token);
						
			getline(ss,token,',');orderReferenceNumber = stol(token);
			oldOrderReferenceNumber = orderReferenceNumber;
			
			if(code=="A" || code=="F")
			{
				getline(ss,indicator,','); 
				getline(ss,token,','); shares = stoi(token);
				getline(ss,stock,',');
				getline(ss,token,','); price = stof(token);
				
				if(code=="F")
					getline(ss,attribution,',');
			}else if(code=="X" || code=="E" || code=="C")
			{
				getline(ss,token,','); canceledShares = stoi(token);
			}else if(code=="U")
			{
				getline(ss,token,','); orderReferenceNumber = stol(token);
				getline(ss,token,','); shares = stoi(token);
				getline(ss,token,','); price = stof(token);
			}				
			
			order x = order(code,stockLocate,trackingNumber,timestamp,orderReferenceNumber,oldOrderReferenceNumber,stock,indicator,shares,price, attribution,canceledShares);
		
			return x;
		}
};

struct compareMin{
	bool operator()(const order& l,const order& r)
	{

		if(l.getXclusiveInfo().getPrice() > r.getXclusiveInfo().getPrice())
		{
			return true;
		}else if(l.getXclusiveInfo().getPrice() < r.getXclusiveInfo().getPrice())
		{
			return false;
		} 		
		else if(l.getOrderReferenceNumber() > r.getOrderReferenceNumber())
		{
			return true;
		}	
		else return false; 
	}
};

struct compareMax{
	bool operator()(const order& l,const order& r)
	{

		if(l.getXclusiveInfo().getPrice() < r.getXclusiveInfo().getPrice())
			return true;
		else if(l.getXclusiveInfo().getPrice() > r.getXclusiveInfo().getPrice())
			return false;
		else if(l.getOrderReferenceNumber() > r.getOrderReferenceNumber())
			return true;
		else return false;
	}
};

static priority_queue<order,vector<order>,compareMax> bid;
static priority_queue<order,vector<order>,compareMin> ask;
static queue<double> bidAskSpread;
unordered_map<long,int> mp;
class orderAction{
	public:
	
		static void takeAction(string s)
		{
			order t = orderFactoryClass::getOrderObj(s);
			
			string messageType = t.getGeneralInfo().getcode();
			string indicator   = t.getXclusiveInfo().getIndicator();
			if(messageType=="A" || messageType=="F")
			{
				
				if(indicator=="B"){ bid.push(t); mp[t.getOrderReferenceNumber()]=1;}
			else {ask.push(t);}
			}else if(messageType=="X" || messageType=="E" || messageType=="C" )
			{
/* 				cout << endl << endl << "*************" << messageType << "********" << endl;
				cout << " shares executed/Cancelled " << t.getXclusiveInfo().getCanceledShares() << endl;
				if(bid.top().getOrderReferenceNumber()==t.getOrderReferenceNumber())
				{
					cout << " orderReference Number " << bid.top().getOrderReferenceNumber();
					cout << endl << " before execution " << bid.top().getXclusiveInfo().getShares() << endl;
					bid.top().getXclusiveInfo().updateShares(bid.top().getXclusiveInfo().getShares() - t.getXclusiveInfo().getCanceledShares());
					cout << endl<< " top bid executed shares "<< t.getXclusiveInfo().getCanceledShares() << endl;
					cout << endl << " after execution " << bid.top().getXclusiveInfo().getShares() << endl;
					bid.top().printOrder();
					if(bid.top().getXclusiveInfo().getShares()<=0)
					{cout <<" fully executed"<<endl;	bid.pop();}
				}else if(ask.top().getOrderReferenceNumber()==t.getOrderReferenceNumber()){
					cout << endl<< " top ask executed shares "<< t.getXclusiveInfo().getCanceledShares() << endl;
					ask.top().getXclusiveInfo().updateShares(t.getXclusiveInfo().getCanceledShares());
					if(ask.top().getXclusiveInfo().getShares()==0)
					{	cout <<" fully executed"<<endl;ask.pop();}
				}else{
					cout << " code cancellation execution error :" << endl;
				} */
				
				//cout << endl << endl << " *********  " << endl << " Executing order:";
				//t.printOrder();
				if(mp[t.getOrderReferenceNumber()])
				{	
					queue<order> q;
					while(!bid.empty()){
						if(bid.top().getOrderReferenceNumber()==t.getOrderReferenceNumber())
						{
							//cout << endl << " before Execution " ; bid.top().printOrder();cout<<endl;
							
							int shares = bid.top().getXclusiveInfo().getShares()-t.getXclusiveInfo().getCanceledShares();
							if(shares){
								order a = order(bid.top().getGeneralInfo().getcode(),bid.top().getGeneralInfo().getStockLocate(),bid.top().getGeneralInfo().getTrackingNumber(),
									bid.top().getGeneralInfo().gettimestamp(),bid.top().getOrderReferenceNumber(),bid.top().getOldOrderReferenceNumber(),bid.top().getXclusiveInfo().getStock(),
									"B",shares,bid.top().getXclusiveInfo().getPrice(),bid.top().getXclusiveInfo().getAttribution(),0);
								q.push(a);
								//cout << "after Execution " ; a.printOrder();cout<<endl;
							}
							
						}else{
							q.push(bid.top());
						}
						bid.pop();
					}
					
					while(!q.empty()) {bid.push(q.front());q.pop();}
					
					
				}else
				{
					queue<order> q;
					while(!ask.empty()){
						if(ask.top().getOrderReferenceNumber()==t.getOrderReferenceNumber())
						{
							int shares = bid.top().getXclusiveInfo().getShares()-t.getXclusiveInfo().getCanceledShares();
							if(shares)
							{
								order a = order(ask.top().getGeneralInfo().getcode(),ask.top().getGeneralInfo().getStockLocate(),ask.top().getGeneralInfo().getTrackingNumber(),
									ask.top().getGeneralInfo().gettimestamp(),ask.top().getOrderReferenceNumber(),ask.top().getOldOrderReferenceNumber(),ask.top().getXclusiveInfo().getStock(),
									"S",ask.top().getXclusiveInfo().getShares()-t.getXclusiveInfo().getCanceledShares(),ask.top().getXclusiveInfo().getPrice(),ask.top().getXclusiveInfo().getAttribution(),0);
								q.push(a);
							}
							
						}else 
						     q.push(ask.top());
						ask.pop();
					}
					
					while(!q.empty()) {ask.push(q.front());q.pop();}

										
				}	
			}else if(messageType=="D")
			{
				/* cout << endl << endl << " *********  " << endl << " Executing order:";
				t.printOrder(); */
			    if(mp[t.getOrderReferenceNumber()])
				{
					queue<order> q;
					
					while(!bid.empty()){
						if(bid.top().getOrderReferenceNumber()!=t.getOrderReferenceNumber())
							q.push(bid.top());
						//else{ cout << " deleted: " << t.getOrderReferenceNumber() << endl;}
						bid.pop();
					}
					
					while(!q.empty()) {bid.push(q.front());q.pop();}
				}else
				{
					queue<order> q;
					while(!ask.empty()){
						if(ask.top().getOrderReferenceNumber()!=t.getOrderReferenceNumber())
							q.push(ask.top());
						//else{ cout << " deleted: " << t.getOrderReferenceNumber() << endl;}
						ask.pop();
					}
					
					while(!q.empty()) {ask.push(q.front());q.pop();}
				}
			}else if(messageType=="U")
			{
				//cout << endl << endl << " *********  " << endl << " Executing order:";
				//t.printOrder();
			    if(mp[t.getOldOrderReferenceNumber()])
				{
					queue<order> q;
					while(!bid.empty()){
						if(bid.top().getOldOrderReferenceNumber()==t.getOldOrderReferenceNumber())
						{
							//cout << "before Exectuion : "; bid.top().printOrder();cout<< endl;
							order a = order(bid.top().getGeneralInfo().getcode(),bid.top().getGeneralInfo().getStockLocate(),bid.top().getGeneralInfo().getTrackingNumber(),
								bid.top().getGeneralInfo().gettimestamp(),t.getOrderReferenceNumber(),bid.top().getOldOrderReferenceNumber(),bid.top().getXclusiveInfo().getStock(),
								"B",t.getXclusiveInfo().getShares(),t.getXclusiveInfo().getPrice(),bid.top().getXclusiveInfo().getAttribution(),0);
							q.push(a);
							mp[a.getOldOrderReferenceNumber()]=0;
							mp[a.getOrderReferenceNumber()] = 1;
							//cout << " updated order: "; a.printOrder(); cout << endl;
						}else q.push(bid.top());
						bid.pop();
					}
					
					while(!q.empty()) {bid.push(q.front());q.pop();}
					
				}else
				{
					//cout << " in else " << endl;
					queue<order> q;
					while(!ask.empty()){
						if(ask.top().getOldOrderReferenceNumber()==t.getOldOrderReferenceNumber())
						{
							//cout << "before Exectuion : "; bid.top().printOrder();cout<< endl;
							order a = order(ask.top().getGeneralInfo().getcode(),ask.top().getGeneralInfo().getStockLocate(),ask.top().getGeneralInfo().getTrackingNumber(),
								ask.top().getGeneralInfo().gettimestamp(),t.getOrderReferenceNumber(),ask.top().getOldOrderReferenceNumber(),ask.top().getXclusiveInfo().getStock(),
								"S",t.getXclusiveInfo().getShares(),t.getXclusiveInfo().getPrice(),ask.top().getXclusiveInfo().getAttribution(),0);
							q.push(a);
							//cout << " updated order: "; a.printOrder();  cout << endl;							
						}else 
						     q.push(ask.top());
						ask.pop();
					}
					
					while(!q.empty()) {ask.push(q.front());q.pop();}

										
				}
			}				
		}
};

int main()
{
	ifstream f("spy.csv");
	string row;

	long count = 0;
	if(f.is_open())
	{
		while(getline(f,row))
		{
			//cout << row << endl;
			if(count%10000==0 || count > 90000)
				cout << count << endl;
			orderAction::takeAction(row);
			++count;

		}
	}
	
	cout << "------output -----"<< endl;	
	while(!bid.empty())
	{
		bid.top().printOrder();
		cout << endl;
		bid.pop();
		
	}
	
	cout << endl << endl << " ask " << endl;
	while(!ask.empty())
	{
		ask.top().printOrder();cout << endl;
		ask.pop();
	}
	
	f.close();
	return 0;
}
