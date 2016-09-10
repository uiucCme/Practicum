
//
//  Message.h
//  itch4Parser
//
//  Created by Dylan Hurd on 11/3/13.
//  Copyright (c) 2013 Dylan Hurd. All rights reserved.
//

#ifndef __itch4Parser__Message__
#define __itch4Parser__Message__

#include <iostream>

using namespace std;

//Consts
// Length = (# of chars in spec)
const int TickerLength = 8;
const int MPIDLength = 4;
const int ReasonLength = 4;
const int issueSubTypeLength = 2;


class Message {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char eventCode;
    public:
    friend istream& operator>> (istream &input,  Message &ts);
    friend ostream& operator<< (ostream &output, Message &ts);
};

class StockDirectory {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char ticker[TickerLength];
    char mktCategory;
    char finStatus;
    unsigned int roundLotSize;
    char roundLotStatus;
    char issueClassification;
    char issueSubtype[issueSubTypeLength];
    char Authenticity;
    char shortSaleAndThresholdIndicator;
    char IPOflag;
    char LULDReferencePriceTier;
    char ETPFlag;
    unsigned int ETPLeverageFactor;
    char inverseIndicator;
    public:
    friend istream& operator>> (istream &input,  StockDirectory &ts);
    friend ostream& operator<< (ostream &output, StockDirectory &ts);
};

class StockTradingAction {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char ticker[TickerLength];
    char tradingState;
    char reserved;
    char reason[ReasonLength];
    public:
    friend istream& operator>> (istream &input,  StockTradingAction &ts);
    friend ostream& operator<< (ostream &output, StockTradingAction &ts);
};

class ShortSalePriceTest {
    private:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char ticker[TickerLength];
    char regSHOAction;
    public:
    friend istream& operator>> (istream &input,  ShortSalePriceTest &ts);
    friend ostream& operator<< (ostream &output, ShortSalePriceTest &ts);
};

class MarketParticipantPosition {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char mpid[MPIDLength];
    char ticker[TickerLength];
    char primaryMarketMaker;
    char marketMakerMode;
    char marketParticipantState;
    public:
    friend istream& operator>> (istream &input,  MarketParticipantPosition &ts);
    friend ostream& operator<< (ostream &output, MarketParticipantPosition &ts);
};

class MWCBDecline{
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    unsigned long level1;
    unsigned long level2;
    unsigned long level3;
    public:
    friend istream& operator>> (istream &input,  MWCBDecline &ts);
    friend ostream& operator<< (ostream &output, MWCBDecline &ts);
    
};

class MWCBStatue{
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char breachedLevel;
    public:
    friend istream& operator>> (istream &input,  MWCBStatue &ts);
    friend ostream& operator<< (ostream &output, MWCBStatue &ts);
    
};

class IPOQuotingPeriodUpdate{
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char ticker[TickerLength];
    unsigned int IPOtime;
    char IPOQualifier;
    unsigned int price;
    public:
    friend istream& operator>> (istream &input,  IPOQuotingPeriodUpdate &ts);
    friend ostream& operator<< (ostream &output, IPOQuotingPeriodUpdate &ts);
    
};


//
// Orders
//
class Order {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    unsigned long refNum; //unique reference number
};

class AddOrder : public Order {
    protected:
    char buyStatus;
    unsigned int quantity;
    char ticker[TickerLength];
    unsigned int price;
    
    public:
    friend istream& operator>> (istream &input,  AddOrder &order);
    friend ostream& operator<< (ostream &output, AddOrder &order);
    
};

class AddMPIDOrder : public AddOrder {
    protected:
    char mpid[MPIDLength];
    public:
    friend istream& operator>> (istream &input,  AddMPIDOrder &order);
    friend ostream& operator<< (ostream &output, AddMPIDOrder &order);
};

class ExecutedOrder : public Order {
    protected:
    unsigned int quantity;
    unsigned long matchNumber;
    public:
    friend istream& operator>> (istream &input,  ExecutedOrder &order);
    friend ostream& operator<< (ostream &output, ExecutedOrder &order);
};

class ExecutedPriceOrder : public ExecutedOrder {
    protected:
    char printable;
    unsigned int price;
    public:
    friend istream& operator>> (istream &input,  ExecutedPriceOrder &order);
    friend ostream& operator<< (ostream &output, ExecutedPriceOrder &order);
};

class CancelOrder : public Order{
    protected:
    unsigned int quantity;
    public:
    friend istream& operator>> (istream &input,  CancelOrder &order);
    friend ostream& operator<< (ostream &output, CancelOrder &order);
};


class DeleteOrder : public Order {
    public:
    friend istream& operator>> (istream &input,  DeleteOrder &order);
    friend ostream& operator<< (ostream &output, DeleteOrder &order);
};

class ReplaceOrder : public Order {
    protected:
    unsigned long newRefNum;
    unsigned int quantity;
    unsigned int price;
    public:
    friend istream& operator>> (istream &input,  ReplaceOrder &order);
    friend ostream& operator<< (ostream &output, ReplaceOrder &order);
};

class TradeMessage : public AddOrder {
    protected:
    unsigned long matchNumber;
    public:
    friend istream& operator>> (istream &input,  TradeMessage &order);
    friend ostream& operator<< (ostream &output, TradeMessage &order);
};

class CrossTradeMessage  {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    unsigned long quantity;
    char ticker[TickerLength];
    unsigned int crossPrice;
    unsigned long matchNumber;
    char crossType;
    public:
    friend istream& operator>> (istream &input,  CrossTradeMessage &order);
    friend ostream& operator<< (ostream &output, CrossTradeMessage &order);
};

//
//trade
//

class BrokenTrade {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    unsigned long matchNumber;
    friend istream& operator>> (istream &input,  BrokenTrade &ts);
    friend ostream& operator<< (ostream &output, BrokenTrade &ts);
};

class NetOrderImbalance {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    unsigned long pairedShares;
    unsigned long imbalanceShares;
    char direction;
    char ticker[TickerLength];
    unsigned int farPrice;
    unsigned int nearPrice;
    unsigned int currentPrice;
    char crossType;
    char priceVar;
    friend istream& operator>> (istream &input,  NetOrderImbalance &ts);
    friend ostream& operator<< (ostream &output, NetOrderImbalance &ts);
};

class RetailPriceImprovement {
    protected:
    unsigned short int stockLocate;
    unsigned short int trackingNumber;
    unsigned long timeStamp;
    char ticker[TickerLength];
    char interest;
    friend istream& operator>> (istream &input,  RetailPriceImprovement &ts);
    friend ostream& operator<< (ostream &output, RetailPriceImprovement &ts);
};


#endif /* defined(__itch4Parser__Message__) */















