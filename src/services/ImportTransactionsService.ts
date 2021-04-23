import { getCustomRepository, getRepository, In } from 'typeorm'
import csvParse from 'csv-parse'
import fs from 'fs'
import Transaction from '../models/Transaction'
import Category from '../models/Category'
import TransactionsRepository from '../repositories/TransactionsRepository'

interface CSVTransaction {
  title: string,
  type: 'income' | 'outcome',
  value: number,
  category: string,
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionsRepository)
    const categoriesRepository = getRepository(Category)

    const transactionsReadStream = fs.createReadStream(filePath)

    const parsers = csvParse({
      from_line: 2,
    })

    const parseCSV = transactionsReadStream.pipe(parsers)

    const transactions: Array<CSVTransaction> = []
    const categories: Array<string> = []

    parseCSV.on('data', async line => {
      const [title, type, value, category] = line.map((cell:string) => cell.trim())

      if (!title || !type || !value) return

      categories.push(category)
      transactions.push({ title, type, value, category })
    })

    await new Promise(resolve => parseCSV.on('end', resolve))

    const existentCategories = await categoriesRepository.find({
      where: {
        title: In(categories),
      }
    })

    const existentCategoriesTitle = existentCategories.map((category: Category) => category.title)

    const addCategoriesTitle = categories.filter(category => !existentCategoriesTitle.includes(category)).filter((value,index,self) => self.indexOf(value) === index)

    const newCategories = categoriesRepository.create(
      addCategoriesTitle.map(title => ({
        title,
      }))
    )

    await categoriesRepository.save(newCategories)

    const finalCategories = [ ...newCategories, ...existentCategories]

    const createdTransactions = transactionsRepository.create(
      transactions.map(transactions => ({
        title: transactions.title,
        type: transactions.type,
        value: transactions.value,
        category: finalCategories.find(
          category => category.title === transactions.category
        )
      }))
    )

    await transactionsRepository.save(createdTransactions)

    await fs.promises.unlink(filePath)

    return createdTransactions
  }
}

export default ImportTransactionsService;
