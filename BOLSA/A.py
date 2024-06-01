from mrjob.job import MRJob

class MaxMinStockPerCompany(MRJob):

    def mapper(self, _, line):
        company, price, date = line.split(',')
        yield company, (float(price), date)

    def reducer(self, company, stocksPrice):
        maxPrice = float('-inf')
        maxDate = None
        stocksPrices = list(stocksPrice)
        stocksPrices.sort()

        for price, date in reversed(stocksPrices):
            if price>=maxPrice:
                maxPrice = price
                maxDate = date
            else:
                break

        yield company, ("Min", stocksPrices[0][0], stocksPrices[0][1])
        yield company, ("max", maxPrice, maxDate)
        

if __name__ == '__main__':
    MaxMinStockPerCompany.run()