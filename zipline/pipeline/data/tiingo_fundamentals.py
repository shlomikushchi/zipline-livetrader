"""
Dataset representing tiingo-fundamental data.
"""
from zipline.utils.numpy_utils import (
    datetime64ns_dtype,
    float64_dtype, 
    categorical_dtype
)

from ..domain import US_EQUITIES
from .dataset import Column, DataSet

class TiingoFundamentals(DataSet):
    """
    :class:`~zipline.pipeline.data.DataSet` containing fundamental-data
    from tiingo https://api.tiingo.com/documentation/fundamentals
    """

    # Because data is forward-filled this field gives the date of the statement
    # issued
    statementDate = Column(dtype=datetime64ns_dtype)

    # Accumulated Other Comprehensive Income (unit $, balanceSheet)
    # Unrealized gains and losses - such as items that have changed in value but 
    # haven't been bought or sold yet
    accoci = Column(dtype=float64_dtype)
    
    # Accounts Payable (unit $, balanceSheet)
    # Short-term debt the company must pay off within a year
    acctPay = Column(dtype=float64_dtype)

    # Accounts Receivable (unit $, balanceSheet)
    # Amount company will receive based on work already completed and delivered
    acctRec = Column(dtype=float64_dtype)

    # Current Assets (unit $, balanceSheet)
    # Assets that can easily be converted to cash and can be used to fund day-to-day 
    # operations
    assetsCurrent = Column(dtype=float64_dtype)

    # Other Assets (unit $, balanceSheet)
    # Assets that cannot be easily converted into cash and where the value of the 
    # item will be realized in a year
    assetsNonCurrent = Column(dtype=float64_dtype)

    # Business Acquisitions & Disposals (unit $, cashFlow)
    # A component of cash flow from investing, representing the net cash inflow (outflow) 
    # associated with the acquisition & disposal of businesses, joint-ventures, 
    # affiliates, and other named investments
    businessAcqDisposals = Column(dtype=float64_dtype)

    # Capital Expenditure (unit $, cashFlow)
    # Money uses to upgrade or acquire property or materials to maintain and increase size
    capex = Column(dtype=float64_dtype)

    # Cash and Equivalents (unit $, balanceSheet)
    # Cash or assets that can be converted to cash immediately
    cashAndEq = Column(dtype=float64_dtype)

    # Consolidated Income (unit $, incomeStatement)
    # The portion of profit or loss for the period, net of income taxes, which is 
    # attributable to the consolidated entity.",
    consolidatedIncome = Column(dtype=float64_dtype)

    # Cost of Revenue (unit $, incomeStatement)
    # The aggregate cost of goods produced and sold and services rendered during the 
    # reporting period
    costRev = Column(dtype=float64_dtype)

    # Total Debt (unit $, balanceSheet)
    debt = Column(dtype=float64_dtype)

    # Current Debt (unit $, balanceSheet)
    # debt due within a year
    debtCurrent = Column(dtype=float64_dtype)

    # Non-Current Debt (unit $, balanceSheet)
    # Longer-term debt, typically greater than a year
    debtNonCurrent = Column(dtype=float64_dtype)

    # Deferred Revenue (unit $, balanceSheet)
    # Revenue that was received, but not yet paid out  including sales, license fees, 
    # and royalties, but excluding interest income
    deferredRev = Column(dtype=float64_dtype)

    # Depreciation, Amortization & Accretion (unit $, cashFlow)
    depamor = Column(dtype=float64_dtype)

    # Deposits (unit $, balanceSheet)
    # Deposit liabilities, both domestic and international including deposits such as 
    # savings deposts
    deposits = Column(dtype=float64_dtype)

    # Earning Before Interest & Taxes EBIT (unit $, incomeStatement)
    ebit = Column(dtype=float64_dtype)

    # EBITDA (unit $, incomeStatement)
    # EBITDA is a non-GAAP accounting metric that is widely used when assessing the 
    # performance of companies, calculated by adding Depreciation/Amortization back to 
    # Earnings before interest and taxes
    ebitda = Column(dtype=float64_dtype)

    # Earnings before tax (unit $,  incomeStatement)
    # Revenue - Expenses and excluding tax
    ebt = Column(dtype=float64_dtype)

    # Enterprise Value (unit $, overview)
    # An alternative to marketcap that's the theoretical takeover price of a company 
    # marketcap + debt - cash and cash equivalents
    enterpriseVal = Column(dtype=float64_dtype)

    # Earnings Per Share (unit $, incomeStatement)
    # Earnings per basic share (not diluted)
    eps = Column(dtype=float64_dtype)

    # Earnings Per Share Diluted (unit $, incomeStatement)
    # EPS for diluted shares
    epsDil = Column(dtype=float64_dtype)

    # Shareholders Equity (unit $, balanceSheet)
    equity = Column(dtype=float64_dtype)

    # Free Cash Flow (unit $, cashFlow)
    # Operating Cash Flow - Capex, How much cash is left over after reinvesting it in 
    # the business. Used as a measure to evaluate financial performance of a company
    freeCashFlow = Column(dtype=float64_dtype)

    # Gross Profit (unit $, incomeStatement)
    # Aggregate revenue [REVENUE] less cost of revenue [COR] directly attributable to 
    # the revenue generation activity
    grossProfit = Column(dtype=float64_dtype)

    # Intangible Assets (unit $, balanceSheet)
    # Goodwill and Intangible Assets
    intangibles = Column(dtype=float64_dtype)

    # Interest Expense (unit $, incomeStatement)
    intexp = Column(dtype=float64_dtype)

    # Inventory (unit $, balanceSheet)
    # A component of Total Assets representing the amount after valuation and reserves 
    # of inventory expected to be sold, or consumed within one year or operating cycle, 
    # if longer
    inventory = Column(dtype=float64_dtype)
    
    # Investments (unit $, balanceSheet)
    # A component of assets representing the total amount of marketable and non-marketable 
    # securties, loans receivable and other invested assets
    investments = Column(dtype=float64_dtype)

    # Investment Acquisitions & Disposals (unit $, cashFlow)
    # A component of cash flow from financing, representing the net cash inflow (outflow) 
    # associated with the acquisition & disposal of investments, including marketable 
    # securities and loan originations
    investmentsAcqDisposals = Column(dtype=float64_dtype)

    # Current Investments (unit $, balanceSheet)
    # The current portion of Investments, reported if the company operates a classified 
    # balance sheet that segments current and non-current assets
    investmentsCurrent = Column(dtype=float64_dtype)

    # Non-Current Investments (unit $, balanceSheet)
    # The non-current portion of investments, reported if the company operates a classified 
    # balance sheet that segments current and non-current assets
    investmentsNonCurrent = Column(dtype=float64_dtype)

    # Issuance or Repayment of Debt Securities (unit $, cashFlow)
    # Representing the net cash inflow (outflow) from issuance (repayment) of debt securities
    issrepayDebt = Column(dtype=float64_dtype)

    # Issuance or Repayment of Equity (unit $, cashFlow)
    # Representing the net cash inflow (outflow) from issuance (repayment) of equity
    issrepayEquity = Column(dtype=float64_dtype)

    # Current Liabilities (unit $, balanceSheet)
    # Debt or liabilities that are due within a year
    liabilitiesCurrent = Column(dtype=float64_dtype)

    # Other Liabilities (unit $, balanceSheet)
    # Long-term liabilities (like debt) that are not due within a year
    liabilitiesNonCurrent = Column(dtype=float64_dtype)

    # Market Capitalization (unit $, overview)
    # Size of the company (shares outstanding * share price)
    marketCap = Column(dtype=float64_dtype)

    # Net Cash Flow to Change in Cash & Cash Equivalents (unit $, cashFlow)
    # Net Cash flow - Total cash minux liabilities
    ncf = Column(dtype=float64_dtype)

    # Net Cash Flow from Financing (unit $, cashFlow)
    # Net Cash flow from financing activities like issuing stock or debt minus dividends or 
    # acquiring debt
    ncff = Column(dtype=float64_dtype)

    # Net Cash Flow from Investing (unit $, cashFlow)
    # Cash flow from investments made by the company
    ncfi = Column(dtype=float64_dtype)

    # Net Cash Flow from Operations (unit $, cashFlow)
    # Cash flow generating from a company's operations
    ncfo = Column(dtype=float64_dtype)

    # Effect of Exchange Rate Changes on Cash (unit $, cashFlow)
    # Cash flow from exchange rate changes
    ncfx = Column(dtype=float64_dtype)

    # Net Income Common Stock (unit $, incomeStatement)
    # Net Income applied to common share holders. Used in calculating EPS (earnings per share)
    netIncComStock = Column(dtype=float64_dtype)

    # Net Income from Discontinued Operations (unit $, incomeStatement)
    netIncDiscOps = Column(dtype=float64_dtype)

    # Net income (unit $, incomeStatement)
    netinc = Column(dtype=float64_dtype)

    # Net Income to Non-Controlling Interests (unit $, incomeStatement)
    # The portion of income which is attributable to non-controlling interest shareholders, 
    # subtracted from consolidated income in order to obtain net income
    nonControllingInterests = Column(dtype=float64_dtype)

    # Operating Expenses (unit $, incomeStatement)
    # Operating expenses represents the total expenditure on SG&A, R&D and other operating 
    # expense items, it excludes cost of revenue
    opex = Column(dtype=float64_dtype)

    # Operating Income (unit $, incomeStatement)
    # Operating income is a measure of financial performance before the deduction of interest 
    # expense, tax expenses and other Non-Operating items. It is calculated as gross profit 
    # minus operating expenses
    opinc = Column(dtype=float64_dtype)

    # Payment of Dividends & Other Cash Distributions (unit $, cashFlow)
    # Representing dividends and dividend equivalents paid on common stock and restricted 
    #stock units.
    payDiv = Column(dtype=float64_dtype)

    # Price to Book Ratio (unit None, overview)
    # Price/Book value per share ratio
    pbRatio = Column(dtype=float64_dtype)
    
    # Price to Earnings Ratio (unit None, overview)
    peRatio = Column(dtype=float64_dtype)

    # Property, Plant & Equipment (unit $, balanceSheet)
    # A component of assets representing the total amount of marketable and non-marketable 
    # securties, loans receivable and other invested assets
    ppeq = Column(dtype=float64_dtype)

    # Preferred Dividends Income Statement Impact (unit $, incomeStatement)
    # Impact of dividend on company's preferred shares
    prefDVDs = Column(dtype=float64_dtype)

    # Accumulated Retained Earnings or Deficit (unit $, balanceSheet)
    # A component of Shareholder's Equity representing the cumulative amount of the entities 
    # undistributed earnings or deficit. May only be reported annually by certain companies, 
    # rather than quarterly
    retainedEarnings = Column(dtype=float64_dtype)

    # Revenue (unit $, incomeStatement)
    revenue = Column(dtype=float64_dtype)

    # Research & Development (unit $, incomeStatement)
    # The aggregate costs incurred in a planned search or critical investigation aimed at 
    # discovery of new knowledge with the hope that such knowledge will be useful in developing 
    # a new product or service
    rnd = Column(dtype=float64_dtype)

    # Shared-based Compensation (unit $, cashFlow)
    # A component of cash flow from operating activities, representing the total amount of 
    # noncash, equity-based employee remuneration. This may include the value of stock or unit 
    # options, amortization of restricted stock or units, and adjustment for officers' compensation. 
    # As noncash, this element is an add back when calculating net cash generated by operating 
    # activities using the indirect method
    sbcomp = Column(dtype=float64_dtype)

    # Selling, General & Administrative (unit $, incomeStatement)
    # The aggregate total costs related to selling a firm's product and services, as well as all 
    # other general and administrative expenses. Direct selling expenses (for example, credit, 
    # warranty, and advertising) are expenses that can be directly linked to the sale of specific 
    # products. Indirect selling expenses are expenses that cannot be directly linked to the sale 
    # of specific products, for example telephone expenses, Internet, and postal charges. General 
    # and administrative expenses include salaries
    sga = Column(dtype=float64_dtype)

    # Shares Outstanding (unit None, balanceSheet)
    sharesBasic = Column(dtype=float64_dtype)

    # Weighted Average Shares Undiluted (unit None, incomeStatement)
    shareswa = Column(dtype=float64_dtype)

    # Weighted Average Shares Diluted (unit None, incomeStatement)
    # Used to calculated diluted EPS. Considers all outstanding common stock plus all instruments 
    # that can be converted into shares (like stock options)
    shareswaDil = Column(dtype=float64_dtype)

    # Tax Assets (unit $, balanceSheet)
    # A component of assets representing tax assets and receivables
    taxAssets = Column(dtype=float64_dtype)

    # Tax Expenses (unit $, incomeStatement)
    taxExp = Column(dtype=float64_dtype)

    # Tax Liabilities (unit $, balanceSheet)
    # A component of liabilities representing outstanding tax liabilities
    taxLiabilities = Column(dtype=float64_dtype)

    # Total Assets (unit $, balanceSheet)
    totalAssets = Column(dtype=float64_dtype)

    # Total Liabilities (unit $, balanceSheet)
    totalLiabilities = Column(dtype=float64_dtype)

    # PEG Ratio (unit None, overview)
    # PEG ratio using the trailing 1 year EPS growth rate in the denominator
    trailingPEG1Y = Column(dtype=float64_dtype)

# Define that this data-set can be used with the us-equities domain
TiingoFundamentalsUS = TiingoFundamentals.specialize(US_EQUITIES)
