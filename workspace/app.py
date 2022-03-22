import sys
from process.tier1.EventStandardization import EventStandardization
from process.tier2.PurchaseByUser import PurchaseByUser
from process.tier2.EventByUser import EventByUser

'''
arg[0] : 프로세스 종류
'''

if __name__=='__main__':
    # 매개변수 받아 process 선택
    # print(sys.argv)
    if sys.argv==None:
        pass
    elif sys.argv[1] == 'eventStandardization':
        eventStandardization = EventStandardization()
        eventStandardization.run()
    elif sys.argv[1] == 'purchaseByUser':
        purchaseByUser = PurchaseByUser()
        purchaseByUser.run()
    elif sys.argv[1] == 'eventByUser':
        eventByUser = EventByUser()
        eventByUser.run()