from mrjob.job import MRJob

class StockUpOrStable(MRJob):

    def mapper(self, _, line):
        company, price, date = line.split(',')
        yield company, (date, float(price))

    def reducer(self, company, stocksPrice):
        lastPrice = float('-inf')
        flag = True
        stockPrices = list(stocksPrice)
        stockPrices.sort()
        
        for _ , price in stockPrices:
            if lastPrice<=price:
                continue
            flag = False    

        yield company, flag

if __name__ == '__main__':
    StockUpOrStable.run()