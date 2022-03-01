import sys
from process.RunPreprocess import RunPreprocess
from process.RunProcessEventDaily import RunProcessEventDaily

'''
arg[0] : 프로세스 종류
'''

def main():

    # 매개변수 받아 process 선택
    print(sys.argv)
    # if sys.argv==None or sys.argv[0] == 'preprocess':
    
    # runPreProcess = RunPreprocess()
    # runPreProcess.run()

    runProcessEventDaily = RunProcessEventDaily()
    runProcessEventDaily.run()

if __name__=='__main__':
    main()