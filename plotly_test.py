# import requests
# my_app_id = 'VtoevPaslzrPZf8CQa-dNQ'
# my_app_secret = '7TSsdRpOoewCaxk_WQm2FJdrCzMd8A'
# auth = requests.auth.HTTPBasicAuth(my_app_id, my_app_secret)
# data={
#     'grant_type': 'password',
#     'username': 'RootUser11',
#     'password': 'Baselnabil2004'
# }
# headers = {
#     'User-Agent': 'MyAPI/0.0.1'
# }

# access_token_res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
# access_token = access_token_res.json()['access_token']
# print(access_token)




import praw

reddit = praw.Reddit(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_SECRET",
    user_agent="YourAppName"
)

# Example: Get top 100 posts from r/dataengineering
subreddit = reddit.subreddit("dataengineering")
posts = []
for post in subreddit.top(limit=100):
    posts.append({
        "title": post.title,
        "score": post.score,
        "id": post.id,
        "url": post.url,
        "created": post.created_utc,
        "comments": post.num_comments
    })
print(posts)