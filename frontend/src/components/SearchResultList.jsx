import React from 'react';
import { Loader2 } from 'lucide-react';
import SongListItem from './SongListItem';

const SearchResultList = ({ query, results }) => {
    return (
        <div className="flex flex-col gap-2 pb-20 px-4">
            <p className="text-gray-400 mb-4 mt-4">Found results for "{query}"</p>
            {results.map((song, idx) => (
                <SongListItem key={`${song._id}-${idx}`} song={song} index={idx} />
            ))}
            
            {isSearching && (
                <div className="py-4 flex justify-center"><Loader2 className="w-6 h-6 animate-spin text-primary-500" /></div>
            )}
            
            {!searchHasMore && results.length > 0 && (
                <p className="text-center text-gray-500 py-6 text-sm">~ End of results ~</p>
            )}
            
            {!isSearching && results.length === 0 && (
                <EmptyState message={`No results found for "${query}"`} />
            )}
        </div>
    );
}

export default SearchResultList;