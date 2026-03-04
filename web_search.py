import requests

def search_duckduckgo(query):
    url = f'https://api.duckduckgo.com/?q={query}&format=json&no_html=1'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f'Failed to fetch search results: {response.status_code}')

if __name__ == '__main__':
    query = 'latest cryptocurrency trends'
    try:
        results = search_duckduckgo(query)
        for result in results.get('Results', []):
            print(f'{result['Title']} - {result['FirstURL']}')
    except Exception as e:
        print(e)
