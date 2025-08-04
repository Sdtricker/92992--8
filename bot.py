from flask import Flask, request, jsonify
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

app = Flask(__name__)

# Aggressive configuration for maximum data
MAX_WORKERS = 15  # More concurrent requests
REQUEST_TIMEOUT = 7  # Fast timeout
MAX_MOBILE_SEARCHES = 100  # Much higher limit
MAX_AADHAAR_SEARCHES = 50  # Higher Aadhaar limit
MAX_TIME_LIMIT = 30  # 30 seconds max for Render

# API endpoints
MOBILE_API_BASE = 'http://35.184.120.24:8000/search/deep-mobile/'
AADHAAR_API_BASE = 'http://35.184.120.24:8001/search_id?aadhaar='

# Thread-safe storage
results_lock = threading.Lock()

def ultra_fast_request(url):
    """Ultra-fast HTTP request optimized for maximum speed"""
    try:
        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={'User-Agent': 'MaxSearch/2.0', 'Connection': 'close'}
        )
        response.raise_for_status()
        return response.json()
    except:
        return None

def clean_mobile(number):
    """Clean and normalize mobile number"""
    if not number:
        return ''
    clean = re.sub(r'[^0-9]', '', str(number))
    clean = re.sub(r'^91', '', clean)
    return clean if len(clean) == 10 and clean[0] in '6789' else ''

def clean_aadhaar(aadhaar):
    """Clean and validate Aadhaar number"""
    if not aadhaar:
        return ''
    clean = re.sub(r'[^0-9]', '', str(aadhaar))
    return clean if len(clean) == 12 else ''

def extract_all_numbers_from_mobile_result(result_data):
    """Extract ALL mobile numbers and Aadhaar from mobile API result"""
    mobile_numbers = set()
    aadhaar_numbers = set()
    all_data = defaultdict(set)
    
    if not result_data:
        return mobile_numbers, aadhaar_numbers, all_data
    
    # Extract from Requested Number Results
    if 'Requested Number Results' in result_data:
        for data in result_data['Requested Number Results']:
            # Mobile numbers
            mobile = clean_mobile(data.get('📱 Mobile', ''))
            if mobile:
                mobile_numbers.add(mobile)
                all_data['telephones'].add(mobile)
            
            # Alt numbers
            alt_mobile = clean_mobile(data.get('📱 Alt Number', ''))
            if alt_mobile:
                mobile_numbers.add(alt_mobile)
                all_data['telephones'].add(alt_mobile)
            
            # Aadhaar
            aadhaar = clean_aadhaar(data.get('🆔 Aadhar Card', ''))
            if aadhaar:
                aadhaar_numbers.add(aadhaar)
                all_data['aadhaar_cards'].add(aadhaar)
            
            # Other data
            name = str(data.get('👤 Name', '')).strip()
            if name and name != 'N/A':
                all_data['names'].add(name)
            
            father_name = str(data.get('👨‍👦 Father Name', '')).strip()
            if father_name and father_name != 'N/A':
                all_data['father_names'].add(father_name)
            
            address = str(data.get('🏠 Full Address', '')).strip()
            if address and address != 'N/A':
                all_data['addresses'].add(address)
            
            email = str(data.get('📧 Email', '')).strip()
            if email and email != 'N/A':
                all_data['emails'].add(email)
            
            region = str(data.get('📞 Sim/State', '')).strip()
            if region and region != 'N/A':
                all_data['regions'].add(region)
    
    # Extract from Alt Numbers section (IMPORTANT - this has more data)
    if 'Also searched full data on Alt Numbers' in result_data:
        for alt_group in result_data['Also searched full data on Alt Numbers']:
            if 'Results' in alt_group:
                for alt_data in alt_group['Results']:
                    # Mobile numbers
                    mobile = clean_mobile(alt_data.get('📱 Mobile', ''))
                    if mobile:
                        mobile_numbers.add(mobile)
                        all_data['telephones'].add(mobile)
                    
                    # Alt numbers
                    alt_mobile = clean_mobile(alt_data.get('📱 Alt Number', ''))
                    if alt_mobile:
                        mobile_numbers.add(alt_mobile)
                        all_data['telephones'].add(alt_mobile)
                    
                    # Aadhaar
                    aadhaar = clean_aadhaar(alt_data.get('🆔 Aadhar Card', ''))
                    if aadhaar:
                        aadhaar_numbers.add(aadhaar)
                        all_data['aadhaar_cards'].add(aadhaar)
                    
                    # Other data
                    name = str(alt_data.get('👤 Name', '')).strip()
                    if name and name != 'N/A':
                        all_data['names'].add(name)
                    
                    father_name = str(alt_data.get('👨‍👦 Father Name', '')).strip()
                    if father_name and father_name != 'N/A':
                        all_data['father_names'].add(father_name)
                    
                    address = str(alt_data.get('🏠 Full Address', '')).strip()
                    if address and address != 'N/A':
                        all_data['addresses'].add(address)
                    
                    email = str(alt_data.get('📧 Email', '')).strip()
                    if email and email != 'N/A':
                        all_data['emails'].add(email)
                    
                    region = str(alt_data.get('📞 Sim/State', '')).strip()
                    if region and region != 'N/A':
                        all_data['regions'].add(region)
    
    return mobile_numbers, aadhaar_numbers, all_data

def extract_all_numbers_from_aadhaar_result(aadhaar_data):
    """Extract ALL mobile numbers and Aadhaar from Aadhaar API result"""
    mobile_numbers = set()
    aadhaar_numbers = set()
    all_data = defaultdict(set)
    
    if not aadhaar_data or 'results' not in aadhaar_data:
        return mobile_numbers, aadhaar_numbers, all_data
    
    for data in aadhaar_data['results']:
        # Mobile numbers
        mobile = clean_mobile(data.get('Mobile Number', ''))
        if mobile:
            mobile_numbers.add(mobile)
            all_data['telephones'].add(mobile)
        
        # Alt numbers
        alt_mobile = clean_mobile(data.get('Alt Number', ''))
        if alt_mobile:
            mobile_numbers.add(alt_mobile)
            all_data['telephones'].add(alt_mobile)
        
        # Aadhaar
        aadhaar = clean_aadhaar(data.get('Aadhaar Card', ''))
        if aadhaar:
            aadhaar_numbers.add(aadhaar)
            all_data['aadhaar_cards'].add(aadhaar)
        
        # Other data
        name = str(data.get('Name', '')).strip()
        if name and name != 'N/A':
            all_data['names'].add(name)
        
        father_name = str(data.get('Father/Husband', '')).strip()
        if father_name and father_name != 'N/A':
            all_data['father_names'].add(father_name)
        
        address = str(data.get('Address', '')).strip()
        if address and address != 'N/A':
            all_data['addresses'].add(address)
        
        email = str(data.get('Email Address', '')).strip()
        if email and email != 'N/A' and email != 'null':
            all_data['emails'].add(email)
        
        region = str(data.get('Sim/State', '')).strip()
        if region and region != 'N/A':
            all_data['regions'].add(region)
    
    return mobile_numbers, aadhaar_numbers, all_data

def search_mobile_numbers_parallel(numbers, searched_numbers):
    """Search multiple mobile numbers in parallel"""
    new_mobile_numbers = set()
    new_aadhaar_numbers = set()
    combined_data = defaultdict(set)
    
    def search_single_mobile(number):
        if number in searched_numbers:
            return None, None, None
        
        result = ultra_fast_request(MOBILE_API_BASE + number)
        if result:
            return extract_all_numbers_from_mobile_result(result)
        return set(), set(), defaultdict(set)
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_number = {executor.submit(search_single_mobile, num): num for num in numbers}
        
        for future in as_completed(future_to_number):
            number = future_to_number[future]
            try:
                mobiles, aadhaar, data = future.result()
                if mobiles is not None and aadhaar is not None and data is not None:
                    searched_numbers.add(number)
                    new_mobile_numbers.update(mobiles)
                    new_aadhaar_numbers.update(aadhaar)
                    
                    # Combine data
                    for key, values in data.items():
                        combined_data[key].update(values)
                        
            except Exception:
                pass
    
    return new_mobile_numbers, new_aadhaar_numbers, combined_data

def search_aadhaar_numbers_parallel(aadhaar_numbers, searched_aadhaar):
    """Search multiple Aadhaar numbers in parallel"""
    new_mobile_numbers = set()
    new_aadhaar_numbers = set()
    combined_data = defaultdict(set)
    
    def search_single_aadhaar(aadhaar):
        if aadhaar in searched_aadhaar:
            return None, None, None
        
        result = ultra_fast_request(AADHAAR_API_BASE + aadhaar)
        if result:
            return extract_all_numbers_from_aadhaar_result(result)
        return set(), set(), defaultdict(set)
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_aadhaar = {executor.submit(search_single_aadhaar, aadhaar): aadhaar for aadhaar in aadhaar_numbers}
        
        for future in as_completed(future_to_aadhaar):
            aadhaar = future_to_aadhaar[future]
            try:
                mobiles, aadhaar_nums, data = future.result()
                if mobiles is not None and aadhaar_nums is not None and data is not None:
                    searched_aadhaar.add(aadhaar)
                    new_mobile_numbers.update(mobiles)
                    new_aadhaar_numbers.update(aadhaar_nums)
                    
                    # Combine data
                    for key, values in data.items():
                        combined_data[key].update(values)
                        
            except Exception:
                pass
    
    return new_mobile_numbers, new_aadhaar_numbers, combined_data

def maximum_recursive_search(initial_number):
    """Maximum recursive search - gets ALL connected numbers"""
    start_time = time.time()
    
    # Initialize
    clean_num = clean_mobile(initial_number)
    if not clean_num:
        return None
    
    # Storage
    all_mobile_numbers = set()
    all_aadhaar_numbers = set()
    combined_data = defaultdict(set)
    searched_numbers = set()
    searched_aadhaar = set()
    
    # Start with initial number
    mobile_queue = {clean_num}
    aadhaar_queue = set()
    
    iteration = 0
    
    # Main recursive loop - continue until no new numbers found
    while (mobile_queue or aadhaar_queue) and iteration < 20 and (time.time() - start_time) < MAX_TIME_LIMIT:
        iteration += 1
        
        # Search mobile numbers
        if mobile_queue:
            # Process up to 15 numbers at once for speed
            current_batch = set(list(mobile_queue)[:15])
            mobile_queue = mobile_queue - current_batch
            
            new_mobiles, new_aadhaar, mobile_data = search_mobile_numbers_parallel(current_batch, searched_numbers)
            
            # Update collections
            all_mobile_numbers.update(new_mobiles)
            all_aadhaar_numbers.update(new_aadhaar)
            
            # Combine data
            for key, values in mobile_data.items():
                combined_data[key].update(values)
            
            # Add new numbers to queues (avoid duplicates)
            for mobile in new_mobiles:
                if mobile not in searched_numbers and mobile not in mobile_queue:
                    mobile_queue.add(mobile)
            
            for aadhaar in new_aadhaar:
                if aadhaar not in searched_aadhaar and aadhaar not in aadhaar_queue:
                    aadhaar_queue.add(aadhaar)
        
        # Search Aadhaar numbers
        if aadhaar_queue and len(searched_aadhaar) < MAX_AADHAAR_SEARCHES:
            # Process up to 8 Aadhaar at once
            current_batch = set(list(aadhaar_queue)[:8])
            aadhaar_queue = aadhaar_queue - current_batch
            
            new_mobiles, new_aadhaar, aadhaar_data = search_aadhaar_numbers_parallel(current_batch, searched_aadhaar)
            
            # Update collections
            all_mobile_numbers.update(new_mobiles)
            all_aadhaar_numbers.update(new_aadhaar)
            
            # Combine data
            for key, values in aadhaar_data.items():
                combined_data[key].update(values)
            
            # Add new mobile numbers back to mobile queue for recursive search
            for mobile in new_mobiles:
                if mobile not in searched_numbers and mobile not in mobile_queue:
                    mobile_queue.add(mobile)
            
            # Add new Aadhaar numbers to queue
            for aadhaar in new_aadhaar:
                if aadhaar not in searched_aadhaar and aadhaar not in aadhaar_queue:
                    aadhaar_queue.add(aadhaar)
        
        # Stop if we've hit search limits
        if len(searched_numbers) >= MAX_MOBILE_SEARCHES:
            break
    
    end_time = time.time()
    
    return {
        'telephones': list(all_mobile_numbers),
        'addresses': list(combined_data['addresses'])[:2],  # First 2 as requested
        'aadhaar_cards': list(all_aadhaar_numbers),
        'emails': list(combined_data['emails']),
        'names': list(combined_data['names']),
        'father_names': list(combined_data['father_names']),
        'regions': list(combined_data['regions']),
        'total_mobile_searches': len(searched_numbers),
        'total_aadhaar_searches': len(searched_aadhaar),
        'iterations': iteration,
        'execution_time': round(end_time - start_time, 2)
    }

@app.route('/', methods=['GET'])
def search_api():
    """Main API endpoint for maximum recursive search"""
    try:
        # Get number parameter
        number = request.args.get('num')
        if not number:
            return jsonify({'error': 'Number parameter required: /?num=91xxxxxxxxxx'}), 400
        
        # Validate number
        clean_number = clean_mobile(number)
        if not clean_number:
            return jsonify({'error': 'Invalid mobile number format'}), 400
        
        # Perform maximum recursive search
        results = maximum_recursive_search(clean_number)
        
        if not results or not results['telephones']:
            return jsonify({'error': 'No data found'}), 404
        
        # Format response
        response_data = []
        
        # Add ALL telephone numbers found
        for tel in results['telephones']:
            response_data.append(f"📞Telephone: {tel}")
        
        # Add addresses (max 2)
        for addr in results['addresses']:
            response_data.append(f"🏘️Address: {addr}")
        
        # Add ALL Aadhaar cards found
        for aadhaar in results['aadhaar_cards']:
            response_data.append(f"🃏 Aadhaar Card: {aadhaar}")
        
        # Add ALL emails found
        for email in results['emails']:
            response_data.append(f"📧 Email: {email}")
        
        # Add ALL names found
        for name in results['names']:
            response_data.append(f"👤Full Name: {name}")
        
        for father_name in results['father_names']:
            response_data.append(f"👨Father Name: {father_name}")
        
        # Add ALL regions found
        for region in results['regions']:
            response_data.append(f"🗺️ Region: {region}")
        
        # Final comprehensive response
        return jsonify({
            'requested_number': clean_number,
            'total_numbers_found': len(results['telephones']),
            'total_emails_found': len(results['emails']),
            'total_aadhaar_found': len(results['aadhaar_cards']),
            'total_mobile_searches': results['total_mobile_searches'],
            'total_aadhaar_searches': results['total_aadhaar_searches'],
            'search_iterations': results['iterations'],
            'execution_time': f"{results['execution_time']}s",
            'results': response_data
        })
        
    except Exception as e:
        return jsonify({'error': 'Server error occurred'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Render"""
    return jsonify({'status': 'healthy', 'service': 'maximum-mobile-search-api'})

if __name__ == '__main__':
    # For Render deployment
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)