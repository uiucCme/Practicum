/* 
Add Order "A"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber, BuySellIndicator, Shares, Stock, 
Price
{0, 1, 2, 3, 4, 5, 6, 7, 8]

Add Order "F"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber, BuySellIndicator, Shares, Stock, 
Price, Attribution
{0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

Modify Order "E"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber, ExecutedShares, MatchNumber
{0, 1, 2, 3, 4, 10, 11]

Executed Order "C"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber, ExecutedShares, MatchNumber, 
Printable, ExecutionPrice
{0, 1, 2, 3, 4, 10, 11, 12, 13]

Cancel Order "X"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber, CanceledShares
{0, 1, 2, 3, 4, 14]

Delete Order "D"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OrderReferenceNumber
{0, 1, 2, 3, 4]

Replace Order "U"
MessageType, StockLocate, TrackingNumber, Timestamp, 
OriginalOrderReferenceNumber(use OrderReferenceNumber spot), 
NewOrderReferenceNumber, Shares, Price
{0, 1, 2, 3, 4, 5, 15, 6, 8]

Total
MessageType 0, StockLocate 1, TrackingNumber 2, Timestamp 3, 
OrderReferenceNumber 4, BuySellIndicator 5, Shares 6, Stock 7, 
Price 8, Attribution 9, ExecutedShares 10, MatchNumber 11, 
Printable 12, ExecutionPrice 13, CanceledShares 14, 
NewOrderReferenceNumber 15
*/

//g++ -std=c++11 -lhdf5 -lhdf5_cpp hdf5Builder.cpp -o builder
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <iomanip>
#include <map>
#include <unordered_map>
#include <ctime>
#include <chrono>
#include "H5Cpp.h"

const int OFFSET_ORDER_A[] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
const int OFFSET_ORDER_F[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
const int OFFSET_ORDER_E[] = {0, 1, 2, 3, 4, 10, 11};
const int OFFSET_ORDER_C[] = {0, 1, 2, 3, 4, 10, 11, 12, 13};
const int OFFSET_ORDER_X[] = {0, 1, 2, 3, 4, 14};
const int OFFSET_ORDER_D[] = {0, 1, 2, 3, 4};
const int OFFSET_ORDER_U[] = {0, 1, 2, 3, 4, 5, 15, 6, 8};
const std::string BINARY_SOURCE_FILE = "data";
const std::string HDF5_OUTPUT_FILE = "hdf5_data.h5";
const std::string MAIN_GROUP_NAME = "/StockSymbols";
const std::string memberStrings[] = {"MessageType", "StockLocate", "TrackingNumber","Timestamp", 
										"OrderReferenceNumber", "BuySellIndicator", "Shares", "Stock", 
										"Price", "Attribution", "ExecutedShares", "MatchNumber", 
										"Printable", "ExecutionPrice", "CanceledShares", 
										"NewOrderReferenceNumber"
									};
const hsize_t WRITE_CHUNK_SIZE = 5000;
const hsize_t INIT_DIMS[] = {WRITE_CHUNK_SIZE};
const hsize_t MAX_DIMS[] = {H5S_UNLIMITED};
const hsize_t CHUNK_DIMS[] = {WRITE_CHUNK_SIZE};
const int DATASET_RANK = 1;
hsize_t OFFSET[] = {0};
int STOP = 0;


struct compound_t {
// 	MessageType 0, StockLocate 1, TrackingNumber 2, Timestamp 3, 
// OrderReferenceNumber 4, BuySellIndicator 5, Shares 6, Stock 7, 
// Price 8, Attribution 9, ExecutedShares 10, MatchNumber 11, 
// Printable 12, ExecutionPrice 13, CanceledShares 14, 
// NewOrderReferenceNumber 15
	char MessageType;
	unsigned short StockLocate;
	unsigned short TrackingNumber;
	unsigned long Timestamp;
	unsigned long OrderReferenceNumber;
	char BuySellIndicator;
	unsigned int Shares;
	char Stock[9];
	float Price;
	char Attribution[5];
	unsigned int ExecutedShares;
	unsigned long MatchNumber;
	char Printable;
	float ExecutionPrice;
	unsigned int CanceledShares;
	unsigned long NewOrderReferenceNumber;
};

H5::CompType COMP_TYPE(sizeof(compound_t)); 

//This map uses stockLocate as keys and stock symbols as values
std::map<unsigned short, std::string> stockLocatorMap;
//This map uses stockLocate as keys and HDF5 dataset pointers as values
//This map is used to track the dataset pointers for all stocks to enable
//Fast retreival of pointers
std::map<unsigned short, H5::DataSet *> dataSetPointerMap;
//This map is like dataSetPointerMap, but it keeps track of all the data spaces
//allocated in our program
std::map<unsigned short, H5::DataSpace *> dataSpacePointerMap;
//This map keeps track of all the file dataspaces allocated in our program.
std::map<unsigned short, H5::DataSpace *> fileSpacePointerMap;
//This map is used to track the row dimension of each stock dataset
std::map<unsigned short, hsize_t> dataSetDimensionMap;
//This map is used to track all the data buffers for all stock datasets
std::map<unsigned short, compound_t*> readBufferMap;
//This map is used to track number of rows that have been written in to the buffer
std::map<unsigned short, int> readBufferTrackMap;

/*
Helper function declarations
*/
unsigned short twoByteCharToShort(unsigned char* buffer);
unsigned int fourByteCharToInt(unsigned char* buffer);
unsigned long sixByteCharToLong(unsigned char* buffer);
unsigned long eightByteCharToLong(unsigned char* buffer);
void stripSpaceForCharArray(unsigned char * charArr, int len);
void extendDataSetSpace(unsigned short stockLocator);
void increaseCounterOrWrite(unsigned short stockLocator);
void writeLastChunkAndCleanUp();

/*
End of helper function declarations
*/

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

void parseAddOrderMessage(std::ifstream& inputFile, char* messageType) {
	AddOrderMessage x;
	inputFile.read((char*)(x.stockLocate), 2);
	
	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);
	
	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.buySellIndicator), 1);
	inputFile.read((char*)(x.shares), 4);
	inputFile.read((char*)((*datarow).Stock), 8);
	inputFile.read((char*)(x.price), 4);
	
	stripSpaceForCharArray((unsigned char*)(*datarow).Stock, 9);
	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).BuySellIndicator = x.buySellIndicator[0];
	(*datarow).Shares = fourByteCharToInt((unsigned char*)x.shares);
	(*datarow).Price = float(fourByteCharToInt((unsigned char*)x.price))/10000.0;

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
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

void parseAddOrderMPIDAttributionMessage(std::ifstream& inputFile, char* messageType) {
	AddOrderMPIDAttributionMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.buySellIndicator), 1);
	inputFile.read((char*)(x.shares), 4);
	inputFile.read((char*)((*datarow).Stock), 8);
	inputFile.read((char*)(x.price), 4);
	inputFile.read((char*)((*datarow).Attribution), 4);

	stripSpaceForCharArray((unsigned char*)(*datarow).Stock, 9);
	stripSpaceForCharArray((unsigned char*)(*datarow).Attribution, 5);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).BuySellIndicator = x.buySellIndicator[0];
	(*datarow).Shares = fourByteCharToInt((unsigned char*)x.shares);
	(*datarow).Price = float(fourByteCharToInt((unsigned char*)x.price))/10000.0;

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
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


void parseOrderExecutedMessage(std::ifstream& inputFile, char* messageType){
	OrderExecutedMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.executedShare), 4);
	inputFile.read((char*)(x.matchNumber), 8);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).ExecutionPrice = fourByteCharToInt((unsigned char*)x.executedShare);
	(*datarow).MatchNumber = eightByteCharToLong((unsigned char*)x.matchNumber);

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
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

void parseOrderExecutedWithPriceMessage(std::ifstream& inputFile, char* messageType) {
	OrderExecutedWithPriceMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.executedShare), 4);
	inputFile.read((char*)(x.matchNumber), 8);
	inputFile.read((char*)(x.printable), 1);
	inputFile.read((char*)(x.executionPrice), 4);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).ExecutionPrice = fourByteCharToInt((unsigned char*)x.executedShare);
	(*datarow).MatchNumber = eightByteCharToLong((unsigned char*)x.matchNumber);
	(*datarow).Printable = x.printable[0];
	(*datarow).ExecutionPrice = float(fourByteCharToInt((unsigned char*)x.executionPrice))/10000.0;
	
	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
}

struct OrderCancelMessage {
	//X
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];	
	unsigned char canceledShares[4];
};	//23

void parseOrderCancelMessage(std::ifstream& inputFile, char* messageType){
	OrderCancelMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.canceledShares), 4);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).CanceledShares = fourByteCharToInt((unsigned char*)x.canceledShares);

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
}

struct OrderDeleteMessage {
	//D
	unsigned char stockLocate[2];
	unsigned char trackingNumber[2];
	unsigned char timeStamp[6];
	unsigned char orderReferenceNumber[8];		
};	//19

void parseOrderDeleteMessage(std::ifstream& inputFile, char* messageType) {
	OrderDeleteMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
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

void parseOrderReplaceMessage(std::ifstream& inputFile, char* messageType) {
	OrderReplaceMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.newOrderReferenceNumber), 8);
	inputFile.read((char*)(x.shares), 4);
	inputFile.read((char*)(x.price), 4);

	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).NewOrderReferenceNumber = eightByteCharToLong((unsigned char*)x.newOrderReferenceNumber);
	(*datarow).Shares = fourByteCharToInt((unsigned char*)x.shares);
	(*datarow).Price = float(fourByteCharToInt((unsigned char*)x.price))/10000.0;

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
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

void parseTradeMessage(std::ifstream& inputFile, char* messageType) {
	TradeMessage x;
	inputFile.read((char*)(x.stockLocate), 2);

	unsigned short stockLocate = twoByteCharToShort((unsigned char*)x.stockLocate);
	compound_t * dataset = readBufferMap[stockLocate];
	compound_t * datarow = &(dataset[readBufferTrackMap[stockLocate]]);

	inputFile.read((char*)(x.trackingNumber), 2);
	inputFile.read((char*)(x.timeStamp), 6);
	inputFile.read((char*)(x.orderReferenceNumber), 8);
	inputFile.read((char*)(x.buySellIndicator), 1);
	inputFile.read((char*)(x.shares), 4);
	inputFile.read((char*)(x.stock), 8);
	inputFile.read((char*)(x.price), 4);
	inputFile.read((char*)(x.matchNumber), 8);

	stripSpaceForCharArray((unsigned char*)(*datarow).Stock, 9);
	(*datarow).MessageType = messageType[0];
	(*datarow).StockLocate = stockLocate;
	(*datarow).TrackingNumber = twoByteCharToShort((unsigned char*)x.trackingNumber);
	(*datarow).Timestamp = sixByteCharToLong((unsigned char*)x.timeStamp);
	(*datarow).OrderReferenceNumber = eightByteCharToLong((unsigned char*)x.orderReferenceNumber);
	(*datarow).BuySellIndicator = x.buySellIndicator[0];
	(*datarow).Shares = fourByteCharToInt((unsigned char*)x.shares);
	(*datarow).Price = float(fourByteCharToInt((unsigned char*)x.price))/10000.0;
	(*datarow).MatchNumber = eightByteCharToLong((unsigned char*)x.matchNumber);

	//increment the counter, and if necessary, we write the data into HDF5 file.
	increaseCounterOrWrite(stockLocate);
}

struct FMap {
	typedef void (*ScriptFunction)(std::ifstream&, char*);
	typedef std::unordered_map<char, ScriptFunction> functionMap;

	functionMap decoderMap;

	FMap() {
		decoderMap.emplace('A', &parseAddOrderMessage);
		decoderMap.emplace('F', &parseAddOrderMPIDAttributionMessage);
		decoderMap.emplace('E', &parseOrderExecutedMessage);
		decoderMap.emplace('C', &parseOrderExecutedWithPriceMessage);
		decoderMap.emplace('X', &parseOrderCancelMessage);
		decoderMap.emplace('D', &parseOrderDeleteMessage);
		decoderMap.emplace('U', &parseOrderReplaceMessage);
		decoderMap.emplace('P', &parseTradeMessage);
	}

	void call(const char & pFunction, std::ifstream& inputFile, char* messageType, unsigned char* buffer)
	{
		auto iter = decoderMap.find(pFunction);
		if(iter == decoderMap.end())
		{
			// std::cout<<pFunction<<std::endl;
			// std::cout<<twoByteCharToShort((unsigned char*)buffer) <<std::endl;
			// return;
			///TO-DO: set file pointer to proper location if
			///the current message is not important
			// std::cout<<inputFile.tellg()<<std::endl;
			inputFile.seekg(twoByteCharToShort(buffer)-1, std::ios::cur);
			return;
		}

		// std::cout<<twoByteCharToShort((unsigned char*)buffer) <<std::endl;
		(*iter->second)(inputFile, messageType);
	}

};

//This function initialize the stockLocatorMap by reading
//all "R" message on the given day. It stops when it reads
//the first "L" message of the day, which is issued after 
//all "R" messages are out. Note: each "R" message contains
//a stock that is trading on NASDAQ on the given day
void stockLocatorMap_init() {
	//input file stream, reading binary input
	std::ifstream inputFile(BINARY_SOURCE_FILE.c_str(), std::ios::in | 
							std::ios::binary);
	unsigned char buffer[2];
	char typeBuffer[1];
	while(inputFile.read((char*)buffer, 2)) {
		inputFile.read((char*)typeBuffer, 1);
		if(typeBuffer[0] == 'L')
			break;
		//If the message type is 'R', we add the (stockLocate, symbol)
		//pair to our stockLocatorMap
		if(typeBuffer[0] == 'R') {
			unsigned char stockSymbol[9];
			unsigned char stockLocator[2];
			inputFile.read((char*)stockLocator, 2);
			inputFile.seekg(8, std::ios::cur);
			inputFile.read((char*)stockSymbol, 8);
			unsigned short stockLocator_s= twoByteCharToShort((unsigned char*)stockLocator);
			stripSpaceForCharArray((unsigned char *)stockSymbol, 9);
			std::stringstream ss;
			ss<<stockSymbol;
			stockLocatorMap[stockLocator_s] = ss.str();
			inputFile.seekg(20, std::ios::cur);
		}
		//If else, then we skip this message 
		else {
			inputFile.seekg(twoByteCharToShort((unsigned char*)buffer) - 1, std::ios::cur);
		}
	}
	inputFile.close();
}

//This method initialize the heirarchy of the HDF5_OUTPUT_FILE
//under group "/stocks/", for each stock that exists in the stockLocatorMap
//we create a new group and name it with the corresponding stock symbol name
void hdf5GroupHierarchy_init() {
	try {
		// Turn off the auto-printing when failure occurs so that we can
		// handle the errors appropriately
		H5::Exception::dontPrint();

		H5::H5File file(HDF5_OUTPUT_FILE, H5F_ACC_TRUNC);
		H5::Group mainGroup(file.createGroup(MAIN_GROUP_NAME));
		for(auto it = stockLocatorMap.cbegin(); it != stockLocatorMap.cend(); it++) {
			H5::Group stockGroup(mainGroup.createGroup(it->second));
		}
	}
	catch (H5::FileIException error) {
		error.printError();
	}
	catch (H5::GroupIException error) {
		error.printError();
    }
}

void compType_init() {
	// Create char array type
	hid_t stockType = H5Tcopy(H5T_C_S1);
	hid_t attributionType = H5Tcopy(H5T_C_S1);
	H5Tset_size(stockType, 9);
	H5Tset_size(attributionType, 5);

	// COMP_TYPE(sizeof(compound_t));
	COMP_TYPE.insertMember(memberStrings[0], HOFFSET(compound_t, MessageType), H5::PredType::NATIVE_CHAR);
	COMP_TYPE.insertMember(memberStrings[1], HOFFSET(compound_t, StockLocate), H5::PredType::NATIVE_USHORT);
	COMP_TYPE.insertMember(memberStrings[2], HOFFSET(compound_t, TrackingNumber), H5::PredType::NATIVE_USHORT);
	COMP_TYPE.insertMember(memberStrings[3], HOFFSET(compound_t, Timestamp), H5::PredType::NATIVE_ULONG);
	COMP_TYPE.insertMember(memberStrings[4], HOFFSET(compound_t, OrderReferenceNumber), H5::PredType::NATIVE_ULONG);
	COMP_TYPE.insertMember(memberStrings[5], HOFFSET(compound_t, BuySellIndicator), H5::PredType::NATIVE_CHAR);
	COMP_TYPE.insertMember(memberStrings[6], HOFFSET(compound_t, Shares), H5::PredType::NATIVE_UINT);
	COMP_TYPE.insertMember(memberStrings[7], HOFFSET(compound_t, Stock), stockType);
	COMP_TYPE.insertMember(memberStrings[8], HOFFSET(compound_t, Price), H5::PredType::NATIVE_FLOAT);
	COMP_TYPE.insertMember(memberStrings[9], HOFFSET(compound_t, Attribution), attributionType);
	COMP_TYPE.insertMember(memberStrings[10], HOFFSET(compound_t, ExecutedShares), H5::PredType::NATIVE_UINT);
	COMP_TYPE.insertMember(memberStrings[11], HOFFSET(compound_t, MatchNumber), H5::PredType::NATIVE_ULONG);
	COMP_TYPE.insertMember(memberStrings[12], HOFFSET(compound_t, Printable), H5::PredType::NATIVE_CHAR);
	COMP_TYPE.insertMember(memberStrings[13], HOFFSET(compound_t, ExecutionPrice), H5::PredType::NATIVE_FLOAT);
	COMP_TYPE.insertMember(memberStrings[14], HOFFSET(compound_t, CanceledShares), H5::PredType::NATIVE_UINT);
	COMP_TYPE.insertMember(memberStrings[15], HOFFSET(compound_t, NewOrderReferenceNumber), H5::PredType::NATIVE_ULONG);

}

int hdf5PopulateData() {
	try{
		/* Initialize all the HDF5 parameters needed*/
		// Turn off the auto-printing when failure occurs so that we can
		// handle the errors appropriately
		H5::Exception::dontPrint();

		// Create a new file using the default property lists. 
		H5::H5File file(HDF5_OUTPUT_FILE, H5F_ACC_RDWR);		
		// Allocate space for each writing of data
		// According to my understanding, once this chunk of space is used up
		// I need to reallocate memory to read the data.
		// H5::DataSpace *dataspace = new H5::DataSpace(DATASET_RANK, INIT_DIMS, MAX_DIMS);

		// Modify dataset creation property to enable chunking
		H5::DSetCreatPropList prop;
		prop.setChunk(DATASET_RANK, CHUNK_DIMS);

		H5::DataSet *dataset;
		H5::DataSpace *dataspace;
		// Initialize datasets for all stocks
		for(auto it = stockLocatorMap.cbegin(); it != stockLocatorMap.cend(); it++) {
			//Get the corresponding group for this stock
			H5::Group group(file.openGroup(MAIN_GROUP_NAME+"/"+it->second));
			//Allocate space for this dataset.
			dataspace = new H5::DataSpace(DATASET_RANK, INIT_DIMS, MAX_DIMS);
			//Craete a pointer to the dataset, if the dataset is full, we will need to extend the dataset
			dataset = new H5::DataSet(group.createDataSet(std::to_string(it->first),COMP_TYPE, *dataspace, prop));
			dataSpacePointerMap[it->first] = dataspace;
			dataSetPointerMap[it->first] = dataset;
			dataSetDimensionMap[it->first] = WRITE_CHUNK_SIZE;
			readBufferMap[it->first] = new compound_t[WRITE_CHUNK_SIZE];
			readBufferTrackMap[it->first] = 0;
			std::memset(readBufferMap[it->first], 0, sizeof(compound_t)*WRITE_CHUNK_SIZE);
		}
		FMap decoder;
		/* Populating data*/
		//input file stream, reading binary input
		std::ifstream inputFile(BINARY_SOURCE_FILE.c_str(), std::ios::in | std::ios::binary);
		unsigned char buffer[2];
		while(inputFile.read((char*)buffer, 2)) {
			char typeBuffer[1];
			inputFile.read((char*)typeBuffer, 1);
			decoder.call(typeBuffer[0], inputFile, typeBuffer, buffer);
		}
		inputFile.close();
		writeLastChunkAndCleanUp();
		prop.close();
		file.close();
	}
	// catch failure caused by the H5File operations
    catch(H5::FileIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return -1;
    }

    // catch failure caused by the DataSet operations
    catch(H5::DataSetIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return -1;
    }

    // catch failure caused by the DataSpace operations
    catch(H5::DataSpaceIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return -1;
    }

    return 0;  // successfully terminated
}


void testStockLocatorMap() {
	stockLocatorMap_init();
	for(auto it = stockLocatorMap.cbegin(); it != stockLocatorMap.cend(); it++) {
		std::cout<< it->first << " " << it->second << std::endl;
	}
}

int main() {
	auto wcts = std::chrono::system_clock::now();

	stockLocatorMap_init();
	compType_init();
	hdf5GroupHierarchy_init();
	hdf5PopulateData();

	std::chrono::duration<double> wctduration = (std::chrono::system_clock::now() - wcts);
	std::cout << "Finished in " << wctduration.count() << " seconds [Wall Clock]" << std::endl;

	// testStockLocatorMap();
	return 0;
}

unsigned short twoByteCharToShort(unsigned char* buffer) {
	unsigned short result = 0;
	result += buffer[0];
	result = (result << 8) + buffer[1];
	return result;
}

void stripSpaceForCharArray(unsigned char * charArr, int len) {
	int i = 0;
	while(i < len) {
		if(charArr[i] ==' ')
			break;
		i++;
	}
	std::memset((void*)(charArr+i), 0, len - i);
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

//Extend the dataspace for the corresponding stock data set of the stock locator
//this function will be called everytime after writing a chunk to the data set
void extendDataSetSpace(unsigned short stockLocator) {
	try{
	/* Initialize all the HDF5 parameters needed*/
	// Turn off the auto-printing when failure occurs so that we can
	// handle the errors appropriately
	H5::Exception::dontPrint();

	//update new dimension of the dataset
	OFFSET[0] = dataSetDimensionMap[stockLocator];
	dataSetDimensionMap[stockLocator] = dataSetDimensionMap[stockLocator] + WRITE_CHUNK_SIZE;
	// std::cout<<"The extended size is: "<<dataSetDimensionMap[stockLocator]<<std::endl;
	//set the new size of dataset 
	(dataSetPointerMap[stockLocator])->extend(&(dataSetDimensionMap[stockLocator]));
	
	//delete the old file data space pointer, we will allocate a new file dataspace pointer
	//to write to new part of the dataset
	delete fileSpacePointerMap[stockLocator];
	//select a hyperslab in extended portion of the data set
	fileSpacePointerMap[stockLocator] = new H5::DataSpace((dataSetPointerMap[stockLocator])->getSpace());	


	fileSpacePointerMap[stockLocator]->selectHyperslab(H5S_SELECT_SET, &WRITE_CHUNK_SIZE, OFFSET);

	//delete the old data space pointer, we will need to allocate a new one for the 
	//new data.
	delete dataSpacePointerMap[stockLocator];
	//allocated new data space and store it in the data space map
	dataSpacePointerMap[stockLocator] = new H5::DataSpace(DATASET_RANK, &WRITE_CHUNK_SIZE, NULL);
	
    }
    // catch failure caused by the H5File operations 
    catch(H5::FileIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }

    // catch failure caused by the DataSet operations
    catch(H5::DataSetIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }

    // catch failure caused by the DataSpace operations
    catch(H5::DataSpaceIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }
}

//This function is called at the end of each data parser.
//It increments the counter of the dataset, which determines where in the
//dataset the next datarow will be added into. If the counter reaches 
//WRITE_CHUNK_SIZE - 1, then it is time for us to write into HDF5 file.
void increaseCounterOrWrite(unsigned short stockLocator) {
	if(readBufferTrackMap[stockLocator] < WRITE_CHUNK_SIZE - 1)
		readBufferTrackMap[stockLocator] = readBufferTrackMap[stockLocator] + 1;
	else {
		try{
		/* Initialize all the HDF5 parameters needed*/
		// Turn off the auto-printing when failure occurs so that we can
		// handle the errors appropriately
		H5::Exception::dontPrint();
		readBufferTrackMap[stockLocator] = 0;

		//if not first time write
		if(dataSetDimensionMap[stockLocator] > WRITE_CHUNK_SIZE) {
			dataSetPointerMap[stockLocator]->write(readBufferMap[stockLocator], COMP_TYPE, *dataSpacePointerMap[stockLocator], *fileSpacePointerMap[stockLocator]);
			// STOP++;
			// std::cout<<stockLocatorMap[stockLocator]<<std::endl;
		}
		//first time writing
		else{
			dataSetPointerMap[stockLocator]->write(readBufferMap[stockLocator], COMP_TYPE);
		}
		extendDataSetSpace(stockLocator);
		std::memset(readBufferMap[stockLocator], 0, sizeof(compound_t)*WRITE_CHUNK_SIZE);
		    // catch failure caused by the H5File operations 
	    }
	    catch(H5::FileIException error)
	    {
			error.printError();
			std::cout<<__LINE__<<std::endl;
			return;
	    }

	    // catch failure caused by the DataSet operations
	    catch(H5::DataSetIException error)
	    {
			error.printError();
			// std::cout<<__LINE__<<std::endl;
			return;
	    }

	    // catch failure caused by the DataSpace operations
	    catch(H5::DataSpaceIException error)
	    {
			error.printError();
			std::cout<<__LINE__<<std::endl;
			return;
	    }
	}
}

//This function is called at the end of the main function.
//it write the last chunk of data of each dataset into the HDF5 file
//then, it free all the memories that are allocated.
void writeLastChunkAndCleanUp() {
	try{
	/* Initialize all the HDF5 parameters needed*/
	// Turn off the auto-printing when failure occurs so that we can
	// handle the errors appropriately
	H5::Exception::dontPrint();
	for(auto it = stockLocatorMap.cbegin(); it != stockLocatorMap.cend(); it++) {
		unsigned short stockLocator = it->first;
		//if not first time write
		if(dataSetDimensionMap[stockLocator] > WRITE_CHUNK_SIZE) {
			dataSetPointerMap[stockLocator]->write(readBufferMap[stockLocator], COMP_TYPE, *dataSpacePointerMap[stockLocator], *fileSpacePointerMap[stockLocator]);
		}
		//first time writing
		else{
			dataSetPointerMap[stockLocator]->write(readBufferMap[stockLocator], COMP_TYPE);
		}
		delete fileSpacePointerMap[stockLocator];
		delete dataSpacePointerMap[stockLocator];
		delete dataSetPointerMap[stockLocator];
		delete readBufferMap[stockLocator];
	}	
	}
    catch(H5::FileIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }

    // catch failure caused by the DataSet operations
    catch(H5::DataSetIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }

    // catch failure caused by the DataSpace operations
    catch(H5::DataSpaceIException error)
    {
		error.printError();
		std::cout<<__LINE__<<std::endl;
		return;
    }
}
