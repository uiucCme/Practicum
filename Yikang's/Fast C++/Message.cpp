//
//  Message.cpp
//  itch4Parser
//
//  Created by Dylan Hurd on 11/3/13.
//  Copyright (c) 2013 Dylan Hurd. All rights reserved.
//
#include <ios>
#include <stdint.h>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <vector>
#include "Message.h"


//Reading input - use to read input from the istream - WORKS ON BINARY
template <class T>
void read(istream &input, T &object){
    T *ptr = &object;
    input.read(reinterpret_cast<char*>(ptr), sizeof(T));
}

void readInt(istream &input, unsigned int &n){
    char c[4];
    for(int i=3; i >= 0; i--) {
        input.read(&c[i], 1);
    }
    unsigned int* ptr = reinterpret_cast<unsigned int*>(c);
    n = *ptr;
}

void readInt2(istream &input, unsigned short int &n){
    char c[2];
    for(int i=1; i >= 0; i--) {
        input.read(&c[i], 1);
    }
    unsigned short int* ptr = reinterpret_cast<unsigned short int*>(c);
    n = *ptr;
}

int readInt6(istream &input, unsigned long &n)
{
    char c[6];
    for(int i=5; i >= 0; i--) {
        input.read(&c[i], 1);
    }
    unsigned long* ptr = reinterpret_cast<unsigned long*>(c);
    n = *ptr;
}


void readLong(istream &input, unsigned long &n){
    char c[8];
    for(int i=7; i >= 0; i--) {
        input.read(&c[i], 1);
    }
    unsigned long* ptr = reinterpret_cast<unsigned long*>(c);
    n = *ptr;
}

//
// Input
//


istream& operator>> (istream &input,  Message &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    read(input, o.eventCode);
    return input;
}

istream& operator>> (istream &input,  StockDirectory &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.ticker, 9);
    read(input, o.mktCategory);
    read(input, o.finStatus);
    readInt(input, o.roundLotSize);
    read(input, o.roundLotStatus);
    read(input, o.issueClassification);
    input.get(o.issueSubtype, 3);
    read(input, o.Authenticity);
    read(input, o.shortSaleAndThresholdIndicator);
    read(input, o.IPOflag);
    read(input, o.LULDReferencePriceTier);
    read(input, o.ETPFlag);
    readInt(input,o.ETPLeverageFactor);
    read(input, o.inverseIndicator);
    return input;
}

istream& operator>> (istream &input,  StockTradingAction &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.ticker, 9);
    read(input, o.tradingState);
    input.ignore();
    read(input, o.reserved);
    input.get(o.reason, 5);
    return input;
}

istream& operator>> (istream &input,  ShortSalePriceTest &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.ticker, 9);
    read(input, o.regSHOAction);
    return input;
}

istream& operator>> (istream &input,  MarketParticipantPosition &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.mpid, 5);
    input.get(o.ticker, 9);
    read(input, o.primaryMarketMaker);
    read(input, o.marketMakerMode);
    read(input, o.marketParticipantState);
    return input;
}

istream& operator>> (istream &input,  MWCBDecline &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.level1);
    readLong(input, o.level2);
    readLong(input, o.level3);
    return input;
}

istream& operator>> (istream &input,  MWCBStatue &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    read(input, o.breachedLevel);
    return input;
}

istream& operator>> (istream &input,  IPOQuotingPeriodUpdate &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.ticker, 9);
    readInt(input,o.IPOtime);
    read(input, o.IPOQualifier);
    readInt(input,o.price);
    return input;
}



istream& operator>> (istream &input,  AddOrder &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    read(input, o.buyStatus);
    readInt(input, o.quantity);
    input.get(o.ticker, 9);
    readInt(input, o.price);
    return input;
}

istream& operator>> (istream &input,  AddMPIDOrder &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    read(input, o.buyStatus);
    readInt(input, o.quantity);
    input.get(o.ticker, 9);
    readInt(input, o.price);
    input.get(o.mpid, 5);
    return input;
}

istream& operator>> (istream &input,  ExecutedOrder &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    readInt(input, o.quantity);
    readLong(input, o.matchNumber);
    return input;
}

istream& operator>> (istream &input,  ExecutedPriceOrder &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    readInt(input, o.quantity);
    readLong(input, o.matchNumber);
    read(input, o.printable);
    readInt(input, o.price);
    return input;
}

istream& operator>> (istream &input,  CancelOrder &o){
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    readInt(input, o.quantity);
    return input;
}


istream& operator>> (istream &input,  DeleteOrder &o){
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    return input;
}

istream& operator>> (istream &input, ReplaceOrder &o){
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input,o.refNum);
    readLong(input, o.newRefNum);
    readInt(input, o.quantity);
    readInt(input, o.price);
    return input;
}

istream& operator>> (istream &input,  TradeMessage &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.refNum);
    read(input, o.buyStatus);
    readInt(input, o.quantity);
    input.get(o.ticker, 9);
    readInt(input, o.price);
    readLong(input, o.matchNumber);
    return input;
}

istream& operator>> (istream &input,  CrossTradeMessage &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.quantity);
    input.get(o.ticker, 9);
    readInt(input, o.crossPrice);
    readLong(input, o.matchNumber);
    read(input, o.crossType);
    return input;
}

istream& operator>> (istream &input,  BrokenTrade &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.matchNumber);
    return input;
}

istream& operator>> (istream &input,  NetOrderImbalance &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    readLong(input, o.pairedShares);
    readLong(input, o.imbalanceShares);
    read(input, o.direction);
    input.get(o.ticker, 9);
    readInt(input, o.farPrice);
    readInt(input, o.nearPrice);
    readInt(input, o.currentPrice);
    read(input, o.crossType);
    read(input, o.priceVar);
    return input;
}

istream& operator>> (istream &input,  RetailPriceImprovement &o) {
    readInt2(input,o.stockLocate);
    readInt2(input,o.trackingNumber);
    readInt6(input,o.timeStamp);
    input.get(o.ticker, 9);
    read(input, o.interest);
    return input;
}

//
// Output
//


ostream& operator<< (ostream &output, Message &o){
    output << "S," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.eventCode << '\n';
    return output;
}

ostream& operator<< (ostream &output, StockDirectory &o){
    output << "R," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.mktCategory << ',' << o.finStatus << ',' << o.roundLotSize << ',' << o.roundLotStatus << ',';
    output.write(o.issueSubtype, issueSubTypeLength);
    output << ',' << o.Authenticity << ',' << o.shortSaleAndThresholdIndicator << ',' << o.IPOflag << ',' << o.LULDReferencePriceTier << ',' << o.ETPFlag << ',' << o.ETPLeverageFactor << ',' << o.inverseIndicator << '\n';
    return output;
}

ostream& operator<< (ostream &output, StockTradingAction &o){
    output << "H," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.tradingState << ',' << o.reserved << ',';
    output.write(o.reason, ReasonLength);
    output << '\n';
    return output;
}

ostream& operator<< (ostream &output, ShortSalePriceTest &o){
    output << "Y," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.regSHOAction << '\n';
    return output;
}

ostream& operator<< (ostream &output, MarketParticipantPosition &o){
    output << "L," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.mpid, MPIDLength);
    output << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.primaryMarketMaker << ',' << o.marketMakerMode << ',' << o.marketParticipantState << '\n';
    return output;
}

ostream& operator<< (ostream &output, MWCBDecline &o){
    output << "V," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.level1 << ',' << o.level2 << ',' << o.level3 << '\n';
    return output;
}

ostream& operator<< (ostream &output, MWCBStatue &o){
    output << "V," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.breachedLevel << '\n';
    return output;
}

ostream& operator<< (ostream &output, IPOQuotingPeriodUpdate &o){
    output << "K," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.IPOtime << ',' << o.IPOQualifier << ',' << o.price << '\n';
    return output;
}


ostream& operator<< (ostream &output, AddOrder &o) {
    output << "A," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.buyStatus << ',' << o.quantity << ',';
    output.write(o.ticker,TickerLength);
    output << ',' << o.price << '\n';
    return output;
}

ostream& operator<< (ostream &output, AddMPIDOrder &o) {
    output << "F," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.buyStatus << ',' << o.quantity << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.price << ',';
    output.write(o.mpid, MPIDLength);
    output << '\n';
    return output;
}

ostream& operator<< (ostream &output, ExecutedOrder &o){
    output << "E," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.quantity << ',' << o.matchNumber << '\n';
    return output;
}

ostream& operator<< (ostream &output, ExecutedPriceOrder &o){
    output << "C," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.quantity << ',' << o.matchNumber << ',' << o.printable << ',' << o.price << '\n';
    return output;
}

ostream& operator<< (ostream &output, CancelOrder &o){
    output << "X," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.quantity << '\n';
    return output;
}

ostream& operator<< (ostream &output, DeleteOrder &o){
    output << "D," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum <<'\n';
    return output;
}

ostream& operator<< (ostream &output, ReplaceOrder &o){
    output << "U," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.newRefNum << ',' << o.quantity << ',' << o.price << '\n';
    return output;
}

ostream& operator<< (ostream &output, TradeMessage &o){
    output << "P," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.refNum << ',' << o.buyStatus << ',' << o.quantity << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.price << ',' << o.matchNumber << '\n';
    return output;
}

ostream& operator<< (ostream &output, CrossTradeMessage &o){
    output << "Q," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.quantity << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.crossPrice << ',' << o.matchNumber << ',' << o.crossType << '\n';
    return output;
}


ostream& operator<< (ostream &output, BrokenTrade &o){
    output << "B," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.matchNumber << '\n';
    return output;
}

ostream& operator<< (ostream &output, NetOrderImbalance &o){
    output << "I," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',' << o.pairedShares << ',' << o.imbalanceShares << ',' << o.direction << ',';
    output.write(o.ticker, TickerLength);
    output << ',' << o.farPrice << ',' << o.nearPrice << ',' << o.currentPrice << ',' << o.crossType << ',' << o.priceVar << '\n';
    return output;
}

ostream& operator<< (ostream &output, RetailPriceImprovement &o){
    output << "N," << o.stockLocate << ',' << o.trackingNumber << ',' << o.timeStamp << ',';
    output.write(o.ticker,TickerLength);
    output << ',' << o.interest << '\n';
    return output;
}




