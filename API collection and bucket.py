from datetime import datetime, timedelta
import csv
import requests
import os
import re

# TMDb API Key
API_KEY = "98bf18186ad7003b8a0484b9d03784e5"
BASE_URL = "https://api.themoviedb.org/3"

# Headers for the CSV
headers = [
    'id', 'title', 'overview', 'release_date', 'runtime', 
    'genres', 'production_companies', 'budget', 'revenue', 
    'popularity', 'vote_average', 'vote_count', 'status', 
    'poster_path', 'backdrop_path', 'language'
]

# Path to save the file
output_path = "/Users/vedanth/Desktop/projects/Movies API Pipeline/raw_movies18.csv"

# Helper function to check if a string is alphabetic
def is_alphabetic(s):
    return bool(re.fullmatch(r"[A-Za-z ]+", s))

def remove_null_values(filename):
    """
    Remove rows with null values from the CSV file.
    """
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = [row for row in reader if all(row.values())]  # Filter out rows with null values

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Removed rows with null values from {filename}. Remaining rows: {len(rows)}")
    except Exception as e:
        print(f"Error removing null values: {e}")

def get_movie_details(movie_id):
    """
    Fetch detailed movie data from TMDb API by movie ID.
    """
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {'api_key': API_KEY, 'language': 'en'}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching details for movie ID {movie_id}: {e}")
        return None

def fetch_movies_by_date_range(start_date, end_date, page=1):
    """
    Fetch movies released within a date range from TMDb API.
    """
    url = f"{BASE_URL}/discover/movie"
    params = {
        'api_key': API_KEY,
        'primary_release_date.gte': start_date,
        'primary_release_date.lte': end_date,
        'sort_by': 'release_date.asc',
        'page': page
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movies from {start_date} to {end_date} (page {page}): {e}")
        return None

def save_to_csv(data, filename):
    """
    Save movie data to a CSV file.
    """
    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            if os.stat(filename).st_size == 0:
                writer.writeheader()  # Write headers only if the file is empty
            writer.writerows(data)
            print(f"Saved {len(data)} movies to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def generate_monthly_intervals(start_date, end_date):
    """
    Generate monthly intervals between two dates.
    """
    intervals = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    while current <= end:
        next_month = current.replace(day=28) + timedelta(days=4)  # Go to next month
        next_month = next_month.replace(day=1)  # Set to the 1st of the next month
        intervals.append((current.strftime("%Y-%m-%d"), min(next_month - timedelta(days=1), end).strftime("%Y-%m-%d")))
        current = next_month
    
    return intervals

def fetch_and_save_movies(start_date, end_date):
    """
    Fetch movies from API and save the raw data to a CSV file.
    """
    intervals = generate_monthly_intervals(start_date, end_date)
    print(f"Generated {len(intervals)} monthly intervals for fetching data.")

    for start, end in intervals:
        print(f"Fetching movies from {start} to {end}")
        page = 1
        total_pages = None  # Initialize to None

        while True:
            movies_response = fetch_movies_by_date_range(start, end, page)
            if not movies_response or 'results' not in movies_response:
                print(f"No results found or error occurred for interval {start} to {end}, page {page}. Exiting loop.")
                break

            # Set total_pages on the first API call
            if total_pages is None:
                total_pages = movies_response.get('total_pages', 0)
                if total_pages == 0:
                    print(f"No pages of data available for the interval {start} to {end}.")
                    break

            # If no movies or we've exceeded the total pages, break the loop
            if not movies_response['results'] or page > total_pages:
                print(f"Reached the end of pages for interval {start} to {end}.")
                break

            movies = movies_response['results']
            data_to_save = []
            for movie in movies:
                movie_details = get_movie_details(movie['id'])
                if not movie_details:
                    continue

                # Validation for alphabetic fields
                title = movie_details.get('title', '')
                genres = ', '.join([genre['name'] for genre in movie_details.get('genres', [])])
                production_companies = ', '.join([company['name'] for company in movie_details.get('production_companies', [])])

                if not (is_alphabetic(title) and 
                        all(is_alphabetic(genre) for genre in genres.split(', ')) and 
                        all(is_alphabetic(company) for company in production_companies.split(', '))):
                    print(f"Skipping movie ID {movie['id']} due to invalid data format.")
                    continue

                # Format the movie data
                data_to_save.append({
                    'id': movie_details.get('id'),
                    'title': title,
                    'overview': movie_details.get('overview'),
                    'release_date': movie_details.get('release_date'),
                    'runtime': f"{movie_details.get('runtime')} minutes" if movie_details.get('runtime') else None,
                    'genres': genres,
                    'production_companies': production_companies,
                    'budget': movie_details.get('budget'),
                    'revenue': movie_details.get('revenue'),
                    'popularity': movie_details.get('popularity'),
                    'vote_average': movie_details.get('vote_average'),
                    'vote_count': movie_details.get('vote_count'),
                    'status': movie_details.get('status'),
                    'poster_path': f"https://image.tmdb.org/t/p/w500{movie_details.get('poster_path')}" if movie_details.get('poster_path') else None,
                    'backdrop_path': f"https://image.tmdb.org/t/p/w500{movie_details.get('backdrop_path')}" if movie_details.get('backdrop_path') else None,
                    'language': movie_details.get('original_language', 'Unknown').title()
                })

            save_to_csv(data_to_save, output_path)
            print(f"Processed page {page}/{total_pages}")

            # Increment the page count
            page += 1

if __name__ == "__main__":
    # Set the date range for fetching movies
    fetch_and_save_movies("2024-01-01", "2024-12-31")

    # Remove null values from the saved CSV
    remove_null_values(output_path)
