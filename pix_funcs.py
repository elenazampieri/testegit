def convert_to_sided_amount(df):

    if df['direction'] == 'Credito':
        return df['amount']
    
    elif df['direction'] == 'Debito':
        return df['amount']*(-1)
    
    else:
        return 9999999999999

def format_national_document_id(df, col_name):
    """ Format national_document_id from object to Int64"""

    df[col_name] = df[col_name].replace({',':'.'}, regex=True).fillna(0).astype(float).astype('Int64')
    
    return df

def create_pix_key(df):

    if (df['operation'] == 'Geral') or (df['operation'] == 'Devolucao'):
        return df['end_to_end_id'] + ',' + str(df['amount']) + ',' + str(df['status']) 
    
    elif (df['operation'] == 'Aporte') or (df['operation'] == 'Retirada'):
        return df['str_pix_control_number'] + ',' + str(df['amount']) + ',' + str(df['status'])
    
    else:
        return null

def get_daily_pix_mismatch(df, day):
    mismatch_dic = {'date': day,
                'pix_in_daily_mismatch':round((df[df['date'] == day]['mismatch_pix_in']*100).iloc[0],6),
                'pix_in_daily_mismatch_amount': (df[df['date'] == day]['mismatch_amount_pix_in']/100).iloc[0],
                'pix_in_nb_mismatch_tx':(df[df['date'] == day]['nb_mismatch_tx_pix_in']).iloc[0],
                'pix_in_amount_mismatch_tx':(df[df['date'] == day]['amount_mismatch_tx_pix_in']/100).iloc[0],
                'pix_in_nb_correction_tx':df[df['date'] == day]['nb_correction_tx_pix_in'].iloc[0],
                'pix_in_amount_correction_tx':(df[df['date'] == day]['amount_correction_tx_pix_in']/100).iloc[0],
                
                'pix_out_daily_mismatch':round((df[df['date'] == day]['mismatch_pix_out']*100).iloc[0],6),
                'pix_out_daily_mismatch_amount': (df[df['date'] == day]['mismatch_amount_pix_out']/100).iloc[0],
                'pix_out_nb_mismatch_tx':(df[df['date'] == day]['nb_mismatch_tx_pix_out']).iloc[0],
                'pix_out_amount_mismatch_tx':(df[df['date'] == day]['amount_mismatch_tx_pix_out']/100).iloc[0],
                'pix_out_nb_correction_tx':df[df['date'] == day]['nb_correction_tx_pix_out'].iloc[0],
                'pix_out_amount_correction_tx':(df[df['date'] == day]['amount_correction_tx_pix_out']/100).iloc[0],

                'pix_in_dashboard_link':'http://tableau.sam-app.ro/#/site/acquisition/views/BankRecon/PIXInRecon',
                'pix_out_dashboard_link':'http://tableau.sam-app.ro/#/site/acquisition/views/BankRecon/PIXOutRecon'
                }

    message = """Date: {date}
PIX In:
    - Daily mismatch: {pix_in_daily_mismatch}% 
    - Daily mismatch amount: R${pix_in_daily_mismatch_amount}
    - # mismatch transactions: {pix_in_nb_mismatch_tx}
    - Amount of mismatch transactions: R${pix_in_amount_mismatch_tx}
    - # corrected transactions: {pix_in_nb_correction_tx}
    - Amount of corrected transactions: {pix_in_amount_correction_tx}
    - Dashboard: {pix_in_dashboard_link} 
    
PIX Out:
    - Daily mismatch: {pix_out_daily_mismatch}% 
    - Daily mismatch amount: R${pix_out_daily_mismatch_amount}
    - # mismatch transactions: {pix_out_nb_mismatch_tx}
    - Amount of mismatch transactions: R${pix_out_amount_mismatch_tx}
    - # corrected transactions: {pix_out_nb_correction_tx}
    - Amount of corrected transactions: {pix_out_amount_correction_tx}
    - Dashboard: {pix_out_dashboard_link}
            
            """.format_map(mismatch_dic)
                                 
    return message

consolidated_pix_query = """
-- 0. Dates from XXXX-XX-XX to yesterday
with
    dates as (
                SELECT 
                    generate_series('2021-06-01'::date 
                                    ,date_trunc('day', now() - interval '1 day')
                                    ,'1 day'::interval
                                    )                                               as date_day
            )

-- 1. JD transactions without same day refunds (not really sure why there are same day refunds)
    -- 1.1 preparing JD tx creating tx_key
    ,jd_transactions as (
                            select
                                date_trunc('day',date_day at time zone 'utc' at time zone 'America/Sao_Paulo')                          as date_day
                                ,end_to_end_id                                                                                          as end_to_end_id
                                ,direction                                                                                              as operation
                                ,amount                                                                                                 as amount
                                ,pix_key                                                                                                as pix_key
                                ,concat(pix_key, ',', date_trunc('day',date_day at time zone 'utc' at time zone 'America/Sao_Paulo'))   as tx_key
                                ,'jd'                                                                                                   as source

                            from br_bank.recon_jd_transactions

                            where 
                                1=1
                                and operation in ('Geral', 'Devolucao')
                                and status not in ('RejeitadoSpiPsp','Rejeitado SPI Psp')
                        )

-- 2. Ledger transactions without same day refunds (response sent to HUB but HUB cancelled transaction for some reason: timeout, internet connection,..)
    -- 2.1 preparing ledger tx creating sided_amount and utc datetime conversion to America/Sao_Paulo
    ,ledger_transactions_aux as (
                                select
                                    date_trunc('day',ledger.utc_datetime at time zone 'utc' at time zone 'America/Sao_Paulo')                                   as date_day
                                    ,pix.end_to_end_id                                                                                                          as end_to_end_id
                                    ,ledger.type                                                                                                                as operation
                                    ,case
                                        when ledger.direction = 'Elixir.BankLedger.Types.Transaction.Direction.TransferOut' then -1
                                        when ledger.direction = 'Elixir.BankLedger.Types.Transaction.Direction.TransferIn' then 1
                                    end * ledger.amount                                                                                                         as sided_amount
                                    ,'ledger'                                                                                                                   as source

                                from br_bank.fct_ledger_transactions                                                                                            as ledger
                                
                                join br_bank.pix_gateway_transactions_pix_gateway_public_transactions pix ON ledger.transfer_id = pix.ledger_transfer_id

                                where
                                    1=1
                                    and ledger.type in ('pix_in', 'pix_out')
                                    and (ledger.utc_datetime at time zone 'utc' at time zone 'America/Sao_Paulo')::date between '2021-01-01'::date and date_trunc('day',now() - interval '1 day') -- From January 2021
                                    and ledger.account_id not in ('d3081db2-c7ed-4187-ad3a-d6bcaf46fce6'  ------ Removing transient accounts
                                                                ,'f265c142-2944-4480-a775-9c1a736d8793'
                                                                ,'6d252a9d-9f1d-4a7c-b9fc-16709c96aec1'
                                                                ,'8bf89396-6eb9-497f-b904-ec9614393684'
                                                                ,'33101719-7fef-4da3-aaaf-4fdfdc3e3145'
                                                                ,'1a270fc2-4737-4288-89cf-c684d4c8f97e'
                                                                ,'3caba501-60c7-47bf-bd4c-d3e933f3d3fb'
                                                                ,'15d7be0b-67b0-4087-97bb-4a76fee07802'
                                                                ,'3d5231c2-f9aa-4624-bf7f-37672a91d100'
                                                                ,'ab4f0a2f-b47a-4b6c-aa67-f6ee8dc38ba9'
                                                                ,'0acca9cc-e51d-4202-866b-bf31bf7f905b'
                                                                ,'035dab84-de70-43df-822e-73e200996059'
                                                                ,'d3c90d32-d735-4f6d-b9d4-b9606a8a014d'
                                                                ,'ed2282db-8114-49f8-8673-90f577891299'
                                                                ,'3036de60-b027-41df-8a27-e6222a6ed1d3'
                                                                )

                            )

    -- 2.2 preparing ledger tx creating pix_key, tx_key and ledger_same_day_refund_key
    ,ledger_transactions as (
                                select
                                        date_day                                                    as date_day
                                        ,end_to_end_id                                              as end_to_end_id
                                        ,operation                                                  as operation
                                        ,sided_amount                                               as amount
                                        ,source                                                     as source
                                        ,concat(end_to_end_id,',', sided_amount)                    as pix_key
                                        ,concat(end_to_end_id,',', sided_amount,',',date_day)       as tx_key
                                        ,concat(end_to_end_id,',', date_day)                        as same_day_refund_ledger_key

                                from ledger_transactions_aux

                            )

    -- 2.3 ledger same day refund transactions to be removed 
    ,same_day_refund_ledger_tx as (
                                select
                                    end_to_end_id                   as end_to_end_id
                                    ,same_day_refund_ledger_key     as same_day_refund_ledger_key
                                    ,sum(amount)                    as amount
                                from ledger_transactions

                                group by 1,2

                                having sum(amount) = 0
                                )

    -- 2.4 ledger tx without same day refunds
    ,ledger_without_same_day_refund_tx as (
                                            select 
                                                    ledger.*
                                            from ledger_transactions ledger
                                        
                                            left join same_day_refund_ledger_tx on same_day_refund_ledger_tx.end_to_end_id = ledger.end_to_end_id
                                        
                                            where
                                                1=1
                                                and same_day_refund_ledger_tx.end_to_end_id is null
                                        )

-- 3. Unmatched transactions (tx in ledger but not in JD and vice versa and removing pix_internal_transfer from ledger which is not a mismatch)
,unmatched_transactions as (    
                                select
                                    coalesce(ledger.tx_key, jd.tx_key)                                                          as tx_key
                                    ,coalesce(ledger.pix_key, jd.pix_key)                                                       as pix_key
                                    ,coalesce(ledger.end_to_end_id, jd.end_to_end_id)                                           as end_to_end_id
                                    ,coalesce(date_trunc('day', ledger.date_day), date_trunc('day', jd.date_day))               as date_day
                                    ,coalesce(ledger.amount,jd.amount)                                                          as amount
                                    ,coalesce(ledger.operation, jd.operation)                                                   as operation
                                    ,coalesce(ledger.source, jd.source)                                                         as source
                                    ,case 
                                        when (ledger.date_day is null or jd.date_day is null)                                                    then 'mismatch'
                                        when (ledger.pix_key=jd.pix_key and date_trunc('day',ledger.date_day) != date_trunc('day',jd.date_day))  then 'correction'
                                        else                                                                                                          'review mismatch classification'
                                    end                                                                                         as mismatch_classification

                                from ledger_without_same_day_refund_tx ledger

                                full outer join jd_transactions jd on (jd.tx_key = ledger.tx_key)-- classify transaction_type

                                where
                                    1=1
                                    and (jd.tx_key is null or ledger.tx_key is null)
                                    and (ledger.operation <> 'pix_internal_transfer' or ledger.operation is null) -- not used to find mismatches
)

-- 4. Consolidations (daily)
    -- 4.1 JD
    ,consolidated_jd as (
                        select
                                date_day                                                            as date_day
                                ,sum(case
                                        when operation = 'Credito' then amount
                                        else 0
                                    end)                                                            as amount_pix_in
                                ,sum(case
                                        when operation = 'Debito' then amount
                                        else 0
                                    end)                                                            as amount_pix_out

                        from jd_transactions

                        group by date_day
                    )

    -- 4.2 Ledger without pix_internal_transfer amount (to calculate mismatch)
    ,consolidated_ledger as (
                        select
                                date_day                                                            as date_day
                                ,sum(case
                                        when operation = 'pix_in' then amount
                                        else 0
                                    end)                                                            as amount_pix_in
                                ,sum(case
                                        when operation = 'pix_out' then amount
                                        else 0
                                    end)                                                            as amount_pix_out

                        from ledger_transactions

                        group by date_day
                    )

    -- 4.3 Unmatched transactions
    ,consolidated_unmatched_tx as (
                                select
                                    dates.date_day                                                                                                              as date_day
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='mismatch' and (operation = 'pix_in' or operation = 'Credito') then 1
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as nb_mismatch_tx_pix_in
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='correction' and (operation = 'pix_in' or operation = 'Credito') then 1
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as nb_correction_tx_pix_in
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='mismatch' and (operation = 'pix_in' or operation = 'Credito') then amount
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as amount_mismatch_tx_pix_in
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='correction' and (operation = 'pix_in' or operation = 'Credito') then amount
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as amount_correction_tx_pix_in
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='mismatch' and (operation = 'pix_out' or operation = 'Debito') then 1
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as nb_mismatch_tx_pix_out
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='correction' and (operation = 'pix_out' or operation = 'Debito') then 1
                                                        else 0
                                                    end
                                                    )
                                                ,0
                                            )                                                                                                                   as nb_correction_tx_pix_out
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='mismatch'and (operation = 'pix_out' or operation = 'Debito') then amount
                                                        else 0
                                                    end
                                                    )
                                                ,0 
                                            )                                                                                                                   as amount_mismatch_tx_pix_out
                                    ,coalesce(
                                                sum(case 
                                                        when mismatch_classification='correction' and (operation = 'pix_out' or operation = 'Debito') then amount
                                                        else 0
                                                    end
                                                    )
                                                ,0  
                                            )                                                                                                                   as amount_correction_tx_pix_out

                                from dates 

                                left join unmatched_transactions on unmatched_transactions.date_day = dates.date_day

                                group by dates.date_day
                            )
    
    select
        dates.date_day                                                                                                              as date_day
        ,consolidated_ledger.amount_pix_in                                                                                          as amount_ledger_pix_in
        ,consolidated_jd.amount_pix_in                                                                                              as amount_jd_pix_in
        ,consolidated_ledger.amount_pix_in-consolidated_jd.amount_pix_in                                                            as mismatch_amount_pix_in
        ,round((consolidated_ledger.amount_pix_in-consolidated_jd.amount_pix_in)::numeric/consolidated_ledger.amount_pix_in,6)      as mismatch_pix_in
        ,unmatched_tx.nb_mismatch_tx_pix_in                                                                                         as nb_mismatch_tx_pix_in
        ,unmatched_tx.nb_correction_tx_pix_in                                                                                       as nb_correction_tx_pix_in
        ,unmatched_tx.amount_mismatch_tx_pix_in                                                                                     as amount_mismatch_tx_pix_in
        ,unmatched_tx.amount_correction_tx_pix_in                                                                                   as amount_correction_tx_pix_in
        
        ,consolidated_ledger.amount_pix_out                                                                                         as amount_ledger_pix_out
        ,consolidated_jd.amount_pix_out                                                                                             as amount_jd_pix_out
        ,consolidated_ledger.amount_pix_out-consolidated_jd.amount_pix_out                                                          as mismatch_amount_pix_out
        ,round((consolidated_ledger.amount_pix_out-consolidated_jd.amount_pix_out)::numeric/consolidated_ledger.amount_pix_out,6)   as mismatch_pix_out
        ,unmatched_tx.nb_mismatch_tx_pix_out                                                                                        as nb_mismatch_tx_pix_out
        ,unmatched_tx.nb_correction_tx_pix_out                                                                                      as nb_correction_tx_pix_out
        ,unmatched_tx.amount_mismatch_tx_pix_out                                                                                    as amount_mismatch_tx_pix_out
        ,unmatched_tx.amount_correction_tx_pix_out                                                                                  as amount_correction_tx_pix_out
    from dates

    left join consolidated_ledger on consolidated_ledger.date_day = dates.date_day
    left join consolidated_jd on consolidated_jd.date_day = dates.date_day
    left join consolidated_unmatched_tx unmatched_tx on unmatched_tx.date_day = dates.date_day 
    order by 1 desc 
"""

weekday_dic = {
                'Monday':    0,
                'Tuesday':   1,
                'Wednesday': 2,
                'Thursday':  3,
                'Friday':    4,
                'Saturday':  5,
                'Sunday':    6
            }
