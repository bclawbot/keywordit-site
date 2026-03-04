import requests

def search_bing(query, subscription_key):
    endpoint = 'https://api.bing.microsoft.com/v7.0/search'
    headers = {'Ocp-Apim-Subscription-Key': subscription_key}
    params = {'q': query, 'textDecorations': True, 'textFormat': 'HTML'}
    response = requests.get(endpoint, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f'Failed to fetch search results: {response.status_code}')

if __name__ == '__main__':
    query = 'latest cryptocurrency trends'
    subscription_key = 'YOUR_BING_API_KEY'  # Replace with your Bing API key
    try:
        results = search_bing(query, subscription_key)
        for result in results.get('webPages', {}).get('value', []):
            print(f'{result[name]} - {result[url]}')
    except Exception as e:
        print(e)
