import numpy as np
from scipy.stats import kendalltau
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def align_paragraphs(original, shuffled):
    """Align paragraphs using TF-IDF (handling cases where content is inconsistent)"""
    vectorizer = TfidfVectorizer().fit(original + shuffled)
    orig_vectors = vectorizer.transform(original)
    shuffled_vectors = vectorizer.transform(shuffled)
    
    similarity_matrix = cosine_similarity(shuffled_vectors, orig_vectors)
    aligned_indices = np.argmax(similarity_matrix, axis=1)
    return aligned_indices.tolist()

def calculate_kendall_tau(original_segments, shuffled_segments):
    # Align paragraphs (assuming both documents have the same number of paragraphs)
    aligned_order = align_paragraphs(original_segments, shuffled_segments)
    
    # Generate ideal order [0,1,2,...n]
    true_order = list(range(len(original_segments)))
    
    # Calculate Kendall's Tau
    tau, _ = kendalltau(true_order, aligned_order)
    return tau

original = [
    "Language links are at the top...", 
    "However, a deal to purchase...",  
    "Later, directly borrowing...",     
    "com. YouTube. Retrieved...",      
    "^ International Legal..."         
]

shuffled = [
    "However, 98231refjuw0193h2a deal to purchase...",  
    "Language links are at the top...",
    "Later, directly borrowing...",      
    "com. YouTube. Retrieved...",       
    "^ International Legal..."          
]

score = calculate_kendall_tau(original, shuffled)
print(f"Kendall's Tau: {score:.3f}")