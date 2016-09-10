from struct import *

class generalInfo:
    def __init__(self,tupl):
        self.code      = decoder(tupl[0])
        self.stkLocate = tupl[1]
        self.tracking  = tupl[2]
        self.timestamp = int.from_bytes(tupl[3], byteorder='big')
        
    def retStr(self):
        return self.code+','+str(self.stkLocate)+','+str(self.tracking)+','+str(self.timestamp)
        
class sytemEventMessage:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.eventCode   = decoder(tupl[4])
    
    def __str__(self):
        return self.generalInfo.retStr()+','+str(self.eventCode)
        
class stockRelatedMessage:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.stock = decoder(tupl[4])
        self.category = decoder(tupl[5])
        self.finStatus = decoder(tupl[6])
        self.roundLotSize = tupl[7]#int.from_bytes(tupl[7], byteorder='big')
        self.roundLotsOnly = decoder(tupl[8])
        self.issueClassification = decoder(tupl[9])
        self.issueSubType = decoder(tupl[10])
        self.authenticity = decoder(tupl[11])
        self.shortSale = decoder(tupl[12])
        self.ipoFlag = decoder(tupl[13])
        self.luld = decoder(tupl[14])
        self.etpFlag = decoder(tupl[15])
        self.etpLeverageFactor = tupl[16]#int.from_bytes(tupl[16], byteorder='big')
        self.inverseIndicator = decoder(tupl[17])
    def __str__(self):
        s  =self.generalInfo.retStr()+','+str(self.stock)
        s +=','+self.category+','+self.finStatus+','+str(self.roundLotSize)+','+self.roundLotsOnly
        s += ','+self.issueClassification+','+self.issueSubType+','+ self.authenticity+','+self.shortSale
        s += ','+self.ipoFlag+','+self.luld+','+self.etpFlag+','+str(self.etpLeverageFactor)+','+self.inverseIndicator
        return s
        
def decoder(s):
    return s.decode('UTF-8').strip()

class stockTradingAction:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.stock = decoder(tupl[4])
        self.TradingState = decoder(tupl[5])
        self.reserved = decoder(tupl[6])
        self.reason = decoder(tupl[7])
        
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.stock)
        s += ','+self.TradingState +','+self.reserved+','+self.reason
        return s

class regShoRestriction:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.stock = decoder(tupl[4])
        self.regSHOAction = decoder(tupl[5])

        
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.stock)
        s += ','+self.regSHOAction
        return s

class marketParticipantPosition:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.mpid = decoder(tupl[4])
        self.stock = decoder(tupl[5])
        self.primaryMarketMaker = decoder(tupl[6])
        self.marketMakerMode = decoder(tupl[7])
        self.marketParticipantStaet = decoder(tupl[8])
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.mpid)
        s += ','+self.stock+','+self.primaryMarketMaker+','+self.marketMakerMode+','+self.marketParticipantStaet
        return s

class markedWideCircuitBreaker:
    def __init__(self,tupl):
        self.generalInfo = generalInfo(tupl)
        self.level1    = tupl[4]/(10**8)
        self.level2    = tupl[5]/(10**8)
        self.level3    = tupl[6]/(10**8)
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.level1)
        s += ','+str(self.level2)+','+str(self.level3)
        return s

class mwcbStatusMessage:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.breachlevel   = decoder(tupl[4])
    def __str__(self):
        s = self.generalInfo.retStr()+','+self.breachlevel
        return s

class ipoQuotingPeriodUpdate:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.stock   = decoder(tupl[4])
        self.ipoQuotationReleaseTime = tupl[5]#int.from_bytes(tupl[5], byteorder='big')
        self.ipQuotationQualifier = decoder(tupl[6])
        self.ipoPrice = tupl[7]//(10**4)
    def __str__(self):
        s = self.generalInfo.retStr()+','+self.stock+','+str(self.ipoQuotationReleaseTime)+','+self.ipQuotationQualifier
        s += ','+str(self.ipoPrice)
        return s

class order:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.orderNumber   = tupl[4]
    
    def __str__(self):
        return self.generalInfo.retStr()+','+str(self.orderNumber)


    
class orderExecutionMessage(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.executedShares = tupl[5]
        self.matchNumber    = tupl[6]
        
    def __str__(self):
        return super().__str__()+','+str(self.executedShares)+','+str(self.matchNumber)

class orderExecutionWithPriceMessage(orderExecutionMessage):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.printable      = tupl[7]
        self.executionPrice = tupl[8]/(10**4)
        
    def __str__(self):
        return super().__str__()+','+str(self.printable)+','+str(self.executionPrice)


class orderCancelMessage(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.cancelledShares = tupl[5]
        
    def __str__(self):
        return super().__str__()+','+str(self.cancelledShares)

class orderDeleteMessage(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        
    def __str__(self):
        return super().__str__()

class brokenTradeMessage(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        
    def __str__(self):
        return super().__str__()    
    
class orderReplaceMessage(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.newOrderNumber = tupl[5]
        self.shares         = tupl[6]
        self.price          = tupl[7]/(10**4)
        
    def __str__(self):
        return super().__str__()+','+str(self.newOrderNumber)+','+str(self.shares)+','+str(self.price)

class addOrderNoMPID(order):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.actionIndicator = decoder(tupl[5])
        self.shares        = tupl[6]
        self.stock         = decoder(tupl[7])
        self.price         = tupl[8]/(10**4)

    def __str__(self):
        s = super().__str__()+','+self.actionIndicator+','+str(self.shares)+','+self.stock
        s += ','+str(self.price)
        return s

class tradeMessage(addOrderNoMPID):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.matchNumber = tupl[9]
    def __str__(self):
        s = super().__str__()+','+str(self.matchNumber)
        return s

class crossTradeMessage:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.shares = tupl[4]
        self.stock = decoder(tupl[5])
        self.crossPrice = tupl[6]/(10**4)
        self.matchNumber = tupl[7]
        self.crossType = decoder(tupl[8])
    
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.shares)+','+self.stock+','+str(self.crossPrice)
        s+= ','+str(self.matchNumber)+','+self.crossType
        return s


class noiiMessage:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.pairedShares  = tupl[4]
        self.imbalance     = tupl[5]
        self.imbalanceDirection = decoder(tupl[6])
        self.stock         = decoder(tupl[7])
        self.farPrice      = tupl[8]/10**4
        self.nearPrice     = tupl[9]/10**4
        self.currentReference = tupl[10]/10**4
        self.crossType     = decoder(tupl[11])
        self.priceVariationIndicator = decoder(tupl[12])
        
    def __str__(self):
        s = self.generalInfo.retStr()+','+str(self.pairedShares)+','+str(self.imbalance)
        s+= ','+self.imbalanceDirection+','+self.stock+','+str(self.farPrice)+','+str(self.nearPrice)
        s+= ','+str(self.currentReference)+','+self.crossType + ','+self.priceVariationIndicator
        return s

class addOrderMPID(addOrderNoMPID):
    def __init__(self,tupl):
        super().__init__(tupl)
        self.attribution   = decoder(tupl[9])

    def __str__(self):
        s = super().__str__()+','+self.attribution
        return s

class rpiiMessage:
    def __init__(self,tupl):
        self.generalInfo   = generalInfo(tupl)
        self.stock = decoder(tupl[4])
        self.interestFlag = decoder(tupl[5])
        
    def __str__(self):
        s = self.generalInfo.retStr()+','+self.stock+','+self.interestFlag        
        


		
		
		
f = open("/home/krum/Downloads/07292016.NASDAQ_ITCH50","rb")
g = open("/home/krum/Downloads/out.csv","w")

msg = ''
count = 0
while True:
    code = f.read(1)
    if not code:
        print("reached end")
        break

    if code==b'S':

        message = sytemEventMessage(unpack('!sHH6sc',code+f.read(11)))
        #print(message)
        codeSet.add(decoder(code))
    elif code==b'R':

        message = (stockRelatedMessage(unpack('!cHH6s8sccIcc2scccccIc',code+f.read(38))))
        #print(message)
        codeSet.add(decoder(code))
        
    elif code==b'H':

        message = stockTradingAction(unpack('!sHH6s8scc4s',code+f.read(24)))
        #print(message)
        codeSet.add(decoder(code))

    elif code==b'Y':

        message = regShoRestriction(unpack('!sHH6s8sc',code+f.read(19)))
        #print(message)
        codeSet.add(decoder(code))
          
    elif code==b'L':

        message = marketParticipantPosition(unpack('!sHH6s4s8sccc',code+f.read(25)))
        #print(message)
        codeSet.add(decoder(code))            
        #break
        
    elif code==b'V':

        message = markedWideCircuitBreaker(unpack('!sHH6sQQQ',code+f.read(34)))
        #print(message)
        #codeSet.add(decoder(code)) 
        break
        
    elif code==b'W':

        message = mwcbStatusMessage(unpack('!sHH6sc',code+f.read(11)))
        print(message)
        break
        codeSet.add(decoder(code))
        
    elif code==b'K':

        message = ipoQuotingPeriodUpdate(unpack('!sHH6s8sIcL',code+f.read(27)))
        print(message)
        break
        codeSet.add(decoder(code))
        
    elif code==b'A':

        message = addOrderNoMPID(unpack('!sHH6sQcI8sL',code+f.read(35)))
        #print(message)
        #break
    elif code==b'F':
        message = addOrderMPID(unpack('!sHH6sQcI8sL4s',code+f.read(39)))
        #print(message)
        #break
    elif code==b'E':
        message = orderExecutionMessage(unpack('!sHH6sQIQ',code+f.read(30)))
        #print(message)
        #break
    elif code==b'C':
        message = orderExecutionWithPriceMessage(unpack('!sHH6sQIQcL',code+f.read(30)))
        print(message)
        break
    elif code==b'X':
        message = orderCancelMessage(unpack('!sHH6sQI',code+f.read(22)))
        #print(message)
        #break
    elif code==b'D':
        message = orderDeleteMessage(unpack('!sHH6sQ',code+f.read(18)))
        #print(message)
        #break
    elif code==b'U':
        message = orderReplaceMessage(unpack('!sHH6sQQIL',code+f.read(34)))
        #print(message)
        #break
    elif code==b'P':
        message = tradeMessage(unpack('!sHH6sQcI8sLQ',code+f.read(43)))
        #print(message)
        #break
    elif code==b'Q':
        message = crossTradeMessage(unpack('!sHH6sQ8sLQc',code+f.read(39)))
        print(message)
        break
    elif code==b'B':
        message = brokenTradeMessage(unpack('!sHH6sQ',code+f.read(18)))
        print(message)
        break
    elif code==b'I':
        message = noiiMessage(unpack('!sHH6sQQc8sLLLcc',code+f.read(49)))
        print(message)
        break
    elif code==b'N':
        message = rpiiMessage(unpack('!sHH6s8sc',code+f.read(19)))
        print(message)
        break
		
