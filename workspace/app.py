import sys
from process.tier1.EventStandardization import EventStandardization

'''
arg[0] : 프로세스 종류
'''

if __name__=='__main__':
    # 매개변수 받아 process 선택
    print(sys.argv)
    # if sys.argv==None or sys.argv[0] == 'preprocess':
    
    EventStandardization = EventStandardization()
    EventStandardization.run()