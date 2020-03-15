import argparse
import Immobilienscout24

#'dffbab93-44e9-41c2-bfff-6bab66c89b6c'
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # key=parser.add_argument('--key', type=str, required=True)
    key='dffbab93-44e9-41c2-bfff-6bab66c89b6c'
    Crsl= Immobilienscout24.Immobilienscout(key)

    Crsl.getSummary()
    Crsl.getList(3)
    Crsl.getData(116386491)
