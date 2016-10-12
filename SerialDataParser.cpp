#include <sys/stat.h>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <map>
#include <unordered_map>
#include <ctime>


unsigned long findFileSize(std::string filename);
unsigned short twoByteCharToShort(unsigned char* buffer);
unsigned int fourByteCharToInt(unsigned char* buffer);
unsigned long sixByteCharToLong(unsigned char* timeStamp);
unsigned long eightByteCharToLong(unsigned char* buffer);
void stripSpaceForStock(unsigned char* stock);

struct SystemEventMessage {
	//S
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char eventCode[1];
};

void parseSystemEventMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	SystemEventMessage x;
	unsigned char buffer[11];
	// inputFile.read((char*)x.stockLocate, 2);
	// inputFile.read((char*)x.trackingNumber, 2);
	// inputFile.read((char*)x.timeStamp, 6);
	// inputFile.read((char*)x.eventCode, 1);
	inputFile.read((char*)buffer, 11);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.eventCode, (const void*)(buffer+10), 1);

	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<
				twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.eventCode[0]<<std::endl;
}

struct StockDirectory {
	//R
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char stock[9];
	char marketCategory[1];
	char financialStatusIndicator[1];
	unsigned char roundLotSize[4]; //int
	char roundLotsOnly[1];
	char issueClassification[1];
	char issueSubType[3];
	char authenticity[1];
	char shortSaleThresholdIndicator[1];
	char ipoFlag[1];
	char luldRefPriceTier[1];
	char etpFlag[1];
	unsigned char etpLeverageFactor[4]; //int
	char inverseIndicator[1];
}; //39

void parseStockDirectory(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	StockDirectory x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.marketCategory), 1);
	// inputFile.read((char*)(x.financialStatusIndicator), 1);
	// inputFile.read((char*)(x.roundLotSize), 4);
	// inputFile.read((char*)(x.roundLotsOnly), 1);
	// inputFile.read((char*)(x.issueClassification), 1);
	// inputFile.read((char*)(x.issueSubType), 2);
	// inputFile.read((char*)(x.authenticity), 1);
	// inputFile.read((char*)(x.shortSaleThresholdIndicator), 1);
	// inputFile.read((char*)(x.ipoFlag), 1);
	// inputFile.read((char*)(x.luldRefPriceTier), 1);
	// inputFile.read((char*)(x.etpFlag), 1);
	// inputFile.read((char*)(x.etpLeverageFactor), 4);
	// inputFile.read((char*)(x.inverseIndicator), 1);
	// x.stock[8] = '\0';
	unsigned char buffer[38];
	inputFile.read((char*)buffer, 38);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.stock, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.marketCategory, (const void*)(buffer+18), 1);
	std::memcpy((void*)x.financialStatusIndicator, (const void*)(buffer+19), 1);
	std::memcpy((void*)x.roundLotSize, (const void*)(buffer+20), 1);
	std::memcpy((void*)x.roundLotsOnly, (const void*)(buffer+21), 4);
	std::memcpy((void*)x.issueClassification, (const void*)(buffer+25), 1);
	std::memcpy((void*)x.issueSubType, (const void*)(buffer+26), 2);
	std::memcpy((void*)x.authenticity, (const void*)(buffer+28), 1);
	std::memcpy((void*)x.shortSaleThresholdIndicator, (const void*)(buffer+29), 1);
	std::memcpy((void*)x.ipoFlag, (const void*)(buffer+30), 1);
	std::memcpy((void*)x.luldRefPriceTier, (const void*)(buffer+31), 1);
	std::memcpy((void*)x.etpFlag, (const void*)(buffer+32), 1);
	std::memcpy((void*)x.etpLeverageFactor, (const void*)(buffer+33), 4);
	std::memcpy((void*)x.inverseIndicator, (const void*)(buffer+37), 1);

	stripSpaceForStock((unsigned char*)x.stock);
	x.issueSubType[2] = '\0';
	outputFile<<messageType[0]<<","<< twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.stock<<","<<x.marketCategory[0]<<","<<
				x.financialStatusIndicator[0]<<","<<fourByteCharToInt((unsigned char *)x.roundLotSize)<<","<<x.roundLotsOnly[0]<<","<<
				x.issueClassification[0]<<","<<x.issueSubType<<","<<x.authenticity[0]<<","<<
				x.shortSaleThresholdIndicator[0]<<","<<x.ipoFlag[0]<<","<<x.luldRefPriceTier[0]<<","<<
				x.etpFlag[0]<<","<<fourByteCharToInt((unsigned char *)x.etpLeverageFactor)<<","<<x.inverseIndicator[0]<<std::endl;
}

struct StockTradingAction {
	//H
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char stock[9];
	char tradingState[1];
	char reserved[1];
	char reason[5];
};

void parseStockTradingAction(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	StockTradingAction x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.tradingState), 1);
	// inputFile.read((char*)(x.reserved), 1);
	// inputFile.read((char*)(x.reason), 4);
	// x.stock[8] = '\0';
	unsigned char buffer[24];
	inputFile.read((char*)buffer, 24);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.stock, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.tradingState, (const void*)(buffer+18), 1);
	std::memcpy((void*)x.reserved, (const void*)(buffer+19), 1);
	std::memcpy((void*)x.reason, (const void*)(buffer+20), 4);
	x.reason[4] = '\0';
	stripSpaceForStock((unsigned char*)x.stock);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.stock<<","<<x.tradingState<<","<<
				x.reserved<<","<<x.reason<<std::endl;
}

struct RegShoRestriction {
	//Y
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char stock[9];
	char regSHOAction[1];
}; //20

void parseRegShoRestriction(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	RegShoRestriction x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.regSHOAction), 1);
	unsigned char buffer[19];
	inputFile.read((char*)buffer, 19);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.stock, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.regSHOAction, (const void*)(buffer+18), 1);
	// x.stock[8] = '\0';	
	stripSpaceForStock((unsigned char*)x.stock);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.stock<<","<<
				x.regSHOAction[0]<<std::endl;
}

struct MarketParticipantPosition {
	//L
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char MPID[5];
	char stock[9];
	char primaryMarketMaker[1];
	char marketMakerMode[1];
	char marketParticipantState[1];
};	//26

void parseMarketParticipantPosition(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	MarketParticipantPosition x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.MPID), 4);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.primaryMarketMaker), 1);
	// inputFile.read((char*)(x.marketMakerMode), 1);
	// inputFile.read((char*)(x.marketParticipantState), 1);
	unsigned char buffer[25];
	inputFile.read((char*)buffer, 25);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.MPID, (const void*)(buffer+10), 4);
	std::memcpy((void*)x.stock, (const void*)(buffer+14), 8);
	std::memcpy((void*)x.primaryMarketMaker, (const void*)(buffer+22), 1);
	std::memcpy((void*)x.marketMakerMode, (const void*)(buffer+23), 1);
	std::memcpy((void*)x.marketParticipantState, (const void*)(buffer+24), 1);
	x.MPID[4] = '\0';
	// x.stock[8] = '\0';	
	stripSpaceForStock((unsigned char*)x.stock);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.MPID<<","<<x.stock<<","<<
				x.primaryMarketMaker[0]<<","<<x.marketMakerMode[0]<<","<<
				x.marketParticipantState[0]<<std::endl;	
}

struct MWCBDeclineLevelMessage {
	//V
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char levelOne[8];
	unsigned char levelTwo[8];
	unsigned char levelThree[8];
};	//35

void parseMWCBDeclineLevelMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	MWCBDeclineLevelMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.levelOne), 8);
	// inputFile.read((char*)(x.levelTwo), 8);
	// inputFile.read((char*)(x.levelThree), 8);

	unsigned char buffer[34];
	inputFile.read((char*)buffer, 34);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.levelOne, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.levelTwo, (const void*)(buffer+18), 8);
	std::memcpy((void*)x.levelThree, (const void*)(buffer+26), 8);
	
	unsigned long levelOne = eightByteCharToLong((unsigned char*)x.levelOne);
	unsigned long levelTwo = eightByteCharToLong((unsigned char*)x.levelTwo);
	unsigned long levelThree = eightByteCharToLong((unsigned char*)x.levelThree);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<
				levelOne/100000000<<"."<<std::setfill('0')<<std::setw(8)<<levelOne%100000000<<
				levelTwo/100000000<<"."<<std::setfill('0')<<std::setw(8)<<levelTwo%100000000<<
				levelThree/100000000<<"."<<std::setfill('0')<<std::setw(8)<<levelThree%100000000<<
				std::endl;
}

struct MWCBBreachMessage {
	//W
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char breachedLevel[1];
};	//12

void parseMWCBBreachMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	MWCBBreachMessage x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.breachedLevel), 1);

	unsigned char buffer[11];
	inputFile.read((char*)buffer, 11);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.breachedLevel, (const void*)(buffer+10), 1);
	
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.breachedLevel[0]<<std::endl;
}

struct IPOQuotingPeriodUpdate {
	//K
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char stock[9];
	unsigned char IPOQuotationReleaseTime[4];
	char IPOQuotationReleaseQualifier[1];
	unsigned char IPOPrice[4];
};	//28

void parseIPOQuotingPeriodUpdate(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType){
	IPOQuotingPeriodUpdate x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.IPOQuotationReleaseTime), 4);
	// inputFile.read((char*)(x.IPOQuotationReleaseQualifier), 1);
	// inputFile.read((char*)(x.IPOPrice), 4);
	// x.stock[8] = '\0';	
	unsigned char buffer[27];
	inputFile.read((char*)buffer, 27);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.stock, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.IPOQuotationReleaseTime, (const void*)(buffer+18), 4);
	std::memcpy((void*)x.IPOQuotationReleaseQualifier, (const void*)(buffer+22), 1);
	std::memcpy((void*)x.IPOPrice, (const void*)(buffer+23), 4);
	
	stripSpaceForStock((unsigned char*)x.stock);
	unsigned int IPOPrice = fourByteCharToInt((unsigned char*)x.IPOPrice);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.stock<<","<<
				fourByteCharToInt((unsigned char*)x.IPOQuotationReleaseTime)<<","<<x.IPOQuotationReleaseQualifier[0]<<","<<
				IPOPrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<IPOPrice%10000<<
				std::endl;
}

struct AddOrderMessage {
	//A
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];
	char buySellIndicator[1];
	char shares[4];
	char stock[9];
	unsigned char price[4];
};	//36

void parseAddOrderMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	AddOrderMessage x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.buySellIndicator), 1);
	// inputFile.read((char*)(x.shares), 4);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.price), 4);
	// x.stock[8] = '\0';
	unsigned char buffer[35];
	inputFile.read((char*)buffer, 35);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.buySellIndicator, (const void*)(buffer+18), 1);
	std::memcpy((void*)x.shares, (const void*)(buffer+19), 4);
	std::memcpy((void*)x.stock, (const void*)(buffer+23), 8);
	std::memcpy((void*)x.price, (const void*)(buffer+31), 4);
	
	stripSpaceForStock((unsigned char*)x.stock);
	unsigned int price = fourByteCharToInt((unsigned char*)x.price);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				x.buySellIndicator[0]<<","<<fourByteCharToInt((unsigned char*)x.shares)<<","<<x.stock<<","<<
				price/10000<<"."<<std::setfill('0')<<std::setw(4)<<price%10000<<
				std::endl;
}

struct AddOrderMPIDAttributionMessage {
	//F
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];
	char buySellIndicator[1];
	unsigned char shares[4];
	char stock[9];
	unsigned char price[4];
	char attribution[5];
};	//40

void parseAddOrderMPIDAttributionMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	AddOrderMPIDAttributionMessage x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.buySellIndicator), 1);
	// inputFile.read((char*)(x.shares), 4);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.price), 4);
	// inputFile.read((char*)(x.attribution), 4);
	// x.stock[8] = '\0';
	unsigned char buffer[39];
	inputFile.read((char*)buffer, 39);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.buySellIndicator, (const void*)(buffer+18), 1);
	std::memcpy((void*)x.shares, (const void*)(buffer+19), 4);
	std::memcpy((void*)x.stock, (const void*)(buffer+23), 8);
	std::memcpy((void*)x.price, (const void*)(buffer+31), 4);
	std::memcpy((void*)x.attribution, (const void*)(buffer+35), 4);
	
	x.attribution[4] = '\0';
	stripSpaceForStock((unsigned char*)x.stock);
	unsigned int price = fourByteCharToInt((unsigned char *)x.price);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				x.buySellIndicator[0]<<","<<fourByteCharToInt((unsigned char *)x.shares)<<","<<x.stock<<","<<
				price/10000<<"."<<std::setfill('0')<<std::setw(4)<<price%10000<<","<<
				x.attribution<<std::endl;
}

struct OrderExecutedMessage {
	//E
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	unsigned char executedShare[4];
	unsigned char matchNumber[8];
};	//31


void parseOrderExecutedMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType){
	OrderExecutedMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.executedShare), 4);
	// inputFile.read((char*)(x.matchNumber), 8);

	unsigned char buffer[30];
	inputFile.read((char*)buffer, 30);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.executedShare, (const void*)(buffer+18), 4);
	std::memcpy((void*)x.matchNumber, (const void*)(buffer+22), 8);

	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				fourByteCharToInt((unsigned char*)x.executedShare)<<","<<eightByteCharToLong((unsigned char*)x.matchNumber)<<std::endl;
}

struct OrderExecutedWithPriceMessage{
	//C
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	unsigned char executedShare[4];
	unsigned char matchNumber[8];
	char printable[1];
	unsigned char executionPrice[4];
};  //36

void parseOrderExecutedWithPriceMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	OrderExecutedWithPriceMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.executedShare), 4);
	// inputFile.read((char*)(x.matchNumber), 8);
	// inputFile.read((char*)(x.printable), 1);
	// inputFile.read((char*)(x.executionPrice), 4);

	unsigned char buffer[35];
	inputFile.read((char*)buffer, 35);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.executedShare, (const void*)(buffer+18), 4);
	std::memcpy((void*)x.matchNumber, (const void*)(buffer+22), 8);
	std::memcpy((void*)x.printable, (const void*)(buffer+30), 1);
	std::memcpy((void*)x.executionPrice, (const void*)(buffer+31), 4);

	unsigned int executionPrice = fourByteCharToInt((unsigned char*)x.executionPrice);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				fourByteCharToInt((unsigned char*)x.executedShare)<<","<<eightByteCharToLong((unsigned char*)x.matchNumber)<<","<<x.printable[0]<<","<<
				executionPrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<executionPrice%10000<<","<<
				std::endl;
}

struct OrderCancelMessage {
	//X
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	unsigned char canceledShares[4];
};	//23

void parseOrderCancelMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType){
	OrderCancelMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.canceledShares), 4);

	unsigned char buffer[22];
	inputFile.read((char*)buffer, 22);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.canceledShares, (const void*)(buffer+18), 4);

	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				fourByteCharToInt((unsigned char*)x.canceledShares)<<std::endl;
}

struct OrderDeleteMessage {
	//D
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];		
};	//19

void parseOrderDeleteMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	OrderDeleteMessage x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);

	unsigned char buffer[18];
	inputFile.read((char*)buffer, 18);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);

	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				std::endl;
}

struct OrderReplaceMessage {
	//U
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	unsigned char newOrderReferenceNumber[8];
	unsigned char shares[4];
	unsigned char price[4];
};	//35

void parseOrderReplaceMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	OrderReplaceMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.newOrderReferenceNumber), 8);
	// inputFile.read((char*)(x.shares), 4);
	// inputFile.read((char*)(x.price), 4);

	unsigned char buffer[34];
	inputFile.read((char*)buffer, 34);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.newOrderReferenceNumber, (const void*)(buffer+18), 8);
	std::memcpy((void*)x.shares, (const void*)(buffer+26), 4);
	std::memcpy((void*)x.price, (const void*)(buffer+30), 4);
	
	unsigned int price = fourByteCharToInt((unsigned char*)x.price);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				eightByteCharToLong((unsigned char*)x.newOrderReferenceNumber)<<","<<fourByteCharToInt((unsigned char*)x.shares)<<","<<
				price/10000<<"."<<std::setfill('0')<<std::setw(4)<<price%10000<<","<<
				std::endl;
}

struct TradeMessage {
	//P
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	char buySellIndicator[1];
	unsigned char shares[4];
	char stock[9];
	unsigned char price[4];
	unsigned char matchNumber[8];
};	//44

void parseTradeMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	TradeMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.orderReferenceNumber), 8);
	// inputFile.read((char*)(x.buySellIndicator), 1);
	// inputFile.read((char*)(x.shares), 4);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.price), 4);
	// inputFile.read((char*)(x.matchNumber), 8);

	unsigned char buffer[43];
	inputFile.read((char*)buffer, 43);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.orderReferenceNumber, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.buySellIndicator, (const void*)(buffer+18), 1);
	std::memcpy((void*)x.shares, (const void*)(buffer+19), 4);
	std::memcpy((void*)x.stock, (const void*)(buffer+23), 8);
	std::memcpy((void*)x.price, (const void*)(buffer+31), 4);
	std::memcpy((void*)x.matchNumber, (const void*)(buffer+35), 8);

	unsigned int price = fourByteCharToInt((unsigned char*)x.price);
	// x.stock[8] = '\0';
	stripSpaceForStock((unsigned char*)x.stock);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.orderReferenceNumber)<<","<<
				x.buySellIndicator[0]<<","<<fourByteCharToInt((unsigned char*)x.shares)<<","<<x.stock<<","<<
				price/10000<<"."<<std::setfill('0')<<std::setw(4)<<price%10000<<","<<
				eightByteCharToLong((unsigned char*)x.matchNumber)<<std::endl;
}

struct CrossTradeMessage {
	//Q
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char shares[8];
	char stock[9];
	unsigned char crossPrice[4];
	unsigned char matchNumber[8];
	char crossType[1];
};	//40

void parseCrossTradeMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	CrossTradeMessage x;
	
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.shares), 4);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.crossPrice), 4);
	// inputFile.read((char*)(x.matchNumber), 8);
	// inputFile.read((char*)(x.crossType), 1);

	unsigned char buffer[39];
	inputFile.read((char*)buffer, 39);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.shares, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.stock, (const void*)(buffer+14), 8);
	std::memcpy((void*)x.crossPrice, (const void*)(buffer+22), 4);
	std::memcpy((void*)x.matchNumber, (const void*)(buffer+26), 8);
	std::memcpy((void*)x.crossType, (const void*)(buffer+34), 1);
	
	// x.stock[8] = '\0';
	stripSpaceForStock((unsigned char*)x.stock);
	unsigned int crossPrice = fourByteCharToInt((unsigned char*)x.crossPrice);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.shares)<<","<<x.stock<<","<<
				crossPrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<crossPrice%10000<<","<<
				eightByteCharToLong((unsigned char*)x.matchNumber)<<","<<x.crossType[0]<<std::endl;
}

struct BrokenTradeMessage {
	//B
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char matchNumber[8];
};	//19

void parseBrokenTradeMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	BrokenTradeMessage x;
	inputFile.read((char*)(x.stockLocate), 2);
	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.matchNumber), 8);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.matchNumber)<<std::endl;
}

struct NOIIMessage {
	//I
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char pairedShares[8];
	unsigned char imbalanceShares[8];
	char imbalanceDirection[1];
	char stock[9];
	unsigned char farPrice[4];
	unsigned char nearPrice[4];
	unsigned char currentReferencePrice[4];
	char crossType[1];
	char priceVariationIndicator[1];
};	//50

void parseNOIIMessage(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	NOIIMessage x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.pairedShares), 8);
	// inputFile.read((char*)(x.imbalanceShares), 8);
	// inputFile.read((char*)(x.imbalanceDirection), 1);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.farPrice), 4);
	// inputFile.read((char*)(x.nearPrice), 4);
	// inputFile.read((char*)(x.currentReferencePrice), 4);
	// inputFile.read((char*)(x.crossType), 1);
	// inputFile.read((char*)(x.priceVariationIndicator), 1);
	// x.stock[8] = '\0';
	unsigned char buffer[49];
	inputFile.read((char*)buffer, 49);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.pairedShares, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.imbalanceShares, (const void*)(buffer+18), 8);
	std::memcpy((void*)x.imbalanceDirection, (const void*)(buffer+26), 1);
	std::memcpy((void*)x.stock, (const void*)(buffer+27), 8);
	std::memcpy((void*)x.farPrice, (const void*)(buffer+35), 4);
	std::memcpy((void*)x.nearPrice, (const void*)(buffer+39), 4);
	std::memcpy((void*)x.currentReferencePrice, (const void*)(buffer+43), 4);
	std::memcpy((void*)x.crossType, (const void*)(buffer+47), 1);
	std::memcpy((void*)x.priceVariationIndicator, (const void*)(buffer+48), 1);
	
	stripSpaceForStock((unsigned char*)x.stock);
	unsigned int farPrice = fourByteCharToInt((unsigned char*)x.farPrice);
	unsigned int nearPrice = fourByteCharToInt((unsigned char*)x.nearPrice);
	unsigned int currentReferencePrice = fourByteCharToInt((unsigned char*)x.currentReferencePrice);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<eightByteCharToLong((unsigned char*)x.pairedShares)<<","<<
				eightByteCharToLong((unsigned char*)x.imbalanceShares)<<","<<x.imbalanceDirection[0]<<","<<x.stock<<","<<
				farPrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<farPrice%10000<<","<<
				nearPrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<nearPrice%10000<<","<<
				currentReferencePrice/10000<<"."<<std::setfill('0')<<std::setw(4)<<currentReferencePrice%10000<<","<<
				x.crossType[0]<<","<<x.priceVariationIndicator[0]<<std::endl;
}

struct RPII {
	//N
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	char stock[9];
	char interestFlag[1];
}; 	//20

void parseRPII(std::ofstream& outputFile, std::ifstream& inputFile, char* messageType) {
	RPII x;
	// inputFile.read((char*)(x.stockLocate), 2);
	// inputFile.read((char*)(x.trackingNumber), 2);
	// inputFile.read((char*)(x.timeStamp), 6);
	// inputFile.read((char*)(x.stock), 8);
	// inputFile.read((char*)(x.interestFlag), 1);
	// x.stock[8] = '\0';
	unsigned char buffer[19];
	inputFile.read((char*)buffer, 19);
	std::memcpy((void*)x.stockLocate, (const void*)buffer, 2);
	std::memcpy((void*)x.trackingNumber, (const void*)(buffer+2), 2);
	std::memcpy((void*)x.timeStamp, (const void*)(buffer+4), 6);
	std::memcpy((void*)x.stock, (const void*)(buffer+10), 8);
	std::memcpy((void*)x.interestFlag, (const void*)(buffer+18), 1);
	
	stripSpaceForStock((unsigned char*)x.stock);
	outputFile<<messageType[0]<<","<<twoByteCharToShort((unsigned char*)x.stockLocate)<<","<<twoByteCharToShort((unsigned char*)x.trackingNumber)<<","<<
				sixByteCharToLong((unsigned char*)x.timeStamp)<<","<<x.stock<<","<<x.interestFlag[0]<<std::endl;
}

struct FMap {
	typedef void (*ScriptFunction)(std::ofstream&, std::ifstream&, char*);
	typedef std::unordered_map<char, ScriptFunction> functionMap;

	functionMap decoderMap;

	FMap() {
		decoderMap.emplace('S', &parseSystemEventMessage);
		decoderMap.emplace('R', &parseStockDirectory);
		decoderMap.emplace('H', &parseStockTradingAction);
		decoderMap.emplace('Y', &parseRegShoRestriction);
		decoderMap.emplace('L', &parseMarketParticipantPosition);
		decoderMap.emplace('V', &parseMWCBDeclineLevelMessage);
		decoderMap.emplace('W', &parseMWCBBreachMessage);
		decoderMap.emplace('K', &parseIPOQuotingPeriodUpdate);
		decoderMap.emplace('A', &parseAddOrderMessage);
		decoderMap.emplace('F', &parseAddOrderMPIDAttributionMessage);
		decoderMap.emplace('E', &parseOrderExecutedMessage);
		decoderMap.emplace('C', &parseOrderExecutedWithPriceMessage);
		decoderMap.emplace('X', &parseOrderCancelMessage);
		decoderMap.emplace('D', &parseOrderDeleteMessage);
		decoderMap.emplace('U', &parseOrderReplaceMessage);
		decoderMap.emplace('P', &parseTradeMessage);
		decoderMap.emplace('Q', &parseCrossTradeMessage);
		decoderMap.emplace('B', &parseBrokenTradeMessage);
		decoderMap.emplace('I', &parseNOIIMessage);
		decoderMap.emplace('N', &parseRPII);
	}

	void call(const char & pFunction, std::ofstream& outputFile, std::ifstream& inputFile, char* messageType, unsigned char* buffer)
	{
		auto iter = decoderMap.find(pFunction);
		if(iter == decoderMap.end())
		{
			std::cout<<pFunction<<std::endl;
			// std::cout<<twoByteCharToShort((unsigned char*)buffer) <<std::endl;
			// return;
		}
		// std::cout<<twoByteCharToShort((unsigned char*)buffer) <<std::endl;
		(*iter->second)(outputFile, inputFile, messageType);
	}

};


int main() {
	clock_t begin = clock();
	FMap decoder;
	std::string inputFileName= "data";
	std::string outputFileName = "parsedData.csv";
	unsigned long filesize = findFileSize(inputFileName);

	//input file stream, reading binary input
	std::ifstream inputFile(inputFileName.c_str(), std::ios::in | std::ios::binary);
	//output file stream, writing ascii characters
	std::ofstream outputFile(outputFileName.c_str(), std::ios::out);
	unsigned char buffer[2];
	while(inputFile.read((char*)buffer, 2)) {
		char typeBuffer[1];
		inputFile.read((char*)typeBuffer, 1);
		decoder.call(typeBuffer[0],outputFile, inputFile, typeBuffer, buffer);
	}
	inputFile.close();
	outputFile.close();
	clock_t end = clock();
	double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
	std::cout<<elapsed_secs<<std::endl;
}

unsigned short twoByteCharToShort(unsigned char* buffer){
	return (unsigned short) ((buffer[0] << 8)| buffer[1]);
}

unsigned int fourByteCharToInt(unsigned char* buffer) {
	unsigned int result = 0;
	for(int i = 0; i < 4; i++) {
		result = (result<<8) + buffer[i];
	}
	return result;
}

unsigned long sixByteCharToLong(unsigned char* timeStamp) {
	unsigned long result = 0;
	for(int i = 0; i < 6; i++) {
		result = (result<<8) + timeStamp[i];
	}
	return result;
}

unsigned long eightByteCharToLong(unsigned char* buffer) {
	unsigned long result = 0;
	for(int i = 0; i < 8; i++) {
		result = (result<<8) + buffer[i];
	}
	return result;
}

unsigned long findFileSize(std::string filename){
	struct stat result;
	unsigned long succeed = stat(filename.c_str(), &result);
	if(succeed == 0)
		return result.st_size;
	else
		return -1;
}

void stripSpaceForStock(unsigned char * stock) {
	int i = 0;
	while(i < 8) {
		if(stock[i] ==' '){
			stock[i] = '\0';
			break;
		}
		i++;
	}
	stock[i] = '\0';
}
