import json
import psycopg2
import requests
from datetime import datetime
import os
import hashlib


# Inisialisasi connection di luar handler untuk connection reuse
conn = None


def generate_stock_id(stock_code, date_obj):
    """Generate unique stock ID from stock_code and date"""
    # Format: STOCKCODE_YYYY-MM-DD (contoh: BBCA_2025-11-25)
    return f"{stock_code}_{date_obj.strftime('%Y-%m-%d')}"


def get_db_connection():
    """Reuse database connection across Lambda invocations"""
    global conn
    
    try:
        if conn is None or conn.closed:
            conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                port=os.environ.get('DB_PORT', '5432'),
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
        return conn
    except Exception as e:
        print(f"Connection error: {str(e)}")
        conn = None
        raise


def lambda_handler(event, context):
    cursor = None
    
    try:
        url = "https://getidx.geo-circle.workers.dev/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not data.get('data'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No data received from API'})
            }
        
        print(f"Successfully fetched {len(data['data'])} records from IDX")
        
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Query INSERT dengan id_stock
        insert_query = """
        INSERT INTO stock_summary (
            id_stock, stock_code, date, stock_name, remarks, previous, 
            open_price, first_trade, high, low, close, change,
            volume, value, frequency, index_individual,
            offer, offer_volume, bid, bid_volume,
            listed_shares, tradeable_shares, weight_for_index,
            foreign_sell, foreign_buy, delisting_date,
            non_regular_volume, non_regular_value, non_regular_frequency
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (stock_code, date) 
        DO UPDATE SET
            id_stock = EXCLUDED.id_stock,
            stock_name = EXCLUDED.stock_name,
            remarks = EXCLUDED.remarks,
            previous = EXCLUDED.previous,
            open_price = EXCLUDED.open_price,
            first_trade = EXCLUDED.first_trade,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            change = EXCLUDED.change,
            volume = EXCLUDED.volume,
            value = EXCLUDED.value,
            frequency = EXCLUDED.frequency,
            index_individual = EXCLUDED.index_individual,
            offer = EXCLUDED.offer,
            offer_volume = EXCLUDED.offer_volume,
            bid = EXCLUDED.bid,
            bid_volume = EXCLUDED.bid_volume,
            foreign_sell = EXCLUDED.foreign_sell,
            foreign_buy = EXCLUDED.foreign_buy,
            updated_at = CURRENT_TIMESTAMP
        """
        
        inserted_count = 0
        
        batch_data = []
        for item in data['data']:
            try:
                date_obj = datetime.strptime(item['Date'], "%Y-%m-%dT%H:%M:%S").date()
                stock_code = item['StockCode']
                
                # Generate id_stock
                id_stock = generate_stock_id(stock_code, date_obj)
                
                batch_data.append((
                    id_stock,  # Field baru di posisi pertama
                    stock_code,
                    date_obj,
                    item['StockName'],
                    item.get('Remarks') or None,
                    item.get('Previous', 0),
                    item.get('OpenPrice', 0),
                    item.get('FirstTrade', 0),
                    item.get('High', 0),
                    item.get('Low', 0),
                    item.get('Close', 0),
                    item.get('Change', 0),
                    item.get('Volume', 0),
                    item.get('Value', 0),
                    item.get('Frequency', 0),
                    item.get('IndexIndividual', 0),
                    item.get('Offer', 0),
                    item.get('OfferVolume', 0),
                    item.get('Bid', 0),
                    item.get('BidVolume', 0),
                    item.get('ListedShares', 0),
                    item.get('TradebleShares', 0),
                    item.get('WeightForIndex', 0),
                    item.get('ForeignSell', 0),
                    item.get('ForeignBuy', 0),
                    item.get('DelistingDate') or None,
                    item.get('NonRegularVolume', 0),
                    item.get('NonRegularValue', 0),
                    item.get('NonRegularFrequency', 0)
                ))
                
            except (ValueError, KeyError) as e:
                print(f"Skipping record due to error: {str(e)}")
                continue
        
        if batch_data:
            cursor.executemany(insert_query, batch_data)
            inserted_count = len(batch_data)
        
        connection.commit()
        
        print(f"Successfully processed {inserted_count} stock records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data processed successfully',
                'records_processed': inserted_count,
                'timestamp': str(datetime.now())
            })
        }
        
    except requests.exceptions.Timeout:
        error_msg = "Request timeout - API took too long to respond"
        print(error_msg)
        return {
            'statusCode': 504,
            'body': json.dumps({'error': error_msg})
        }
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error {e.response.status_code}: {str(e)}"
        print(error_msg)
        return {
            'statusCode': e.response.status_code,
            'body': json.dumps({'error': error_msg})
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
        
    except psycopg2.DatabaseError as e:
        error_msg = f"Database error: {str(e)}"
        print(error_msg)
        if conn:
            conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(error_msg)
        if conn:
            conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
        
    finally:
        if cursor:
            cursor.close()
