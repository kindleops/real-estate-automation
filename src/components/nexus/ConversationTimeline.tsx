import React from 'react';
import { Turn } from '../../types/nexus';

export const ConversationTimeline: React.FC = () => {
  return (
    <div className="p-4 bg-gray-900 text-white rounded-lg shadow-md border border-gray-700 h-full">
      <h2 className="text-xl font-bold mb-4 text-blue-400">Conversation Timeline</h2>
      <div className="space-y-4 border-l-2 border-gray-700 pl-4 ml-2">
        {/* Stub for timeline events */}
        <div className="relative">
            <div className="absolute -left-[21px] top-1 h-3 w-3 rounded-full bg-blue-500"></div>
            <p className="text-sm text-gray-300">Awaiting deterministic replay stream...</p>
        </div>
      </div>
      <p className="mt-4 text-sm text-gray-400">Visualizing inbound/outbound turns, detected intents, seller state changes, and AI actions...</p>
    </div>
  );
};
