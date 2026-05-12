import React from 'react';
import { Thread } from '../../types/nexus';

export const LiveNegotiationRadar: React.FC = () => {
  return (
    <div className="p-4 bg-gray-900 text-white rounded-lg shadow-md border border-gray-700">
      <h2 className="text-xl font-bold mb-4 text-cyan-400">Live Negotiation Radar</h2>
      <div className="animate-pulse flex space-x-4">
        <div className="flex-1 space-y-4 py-1">
          <div className="h-4 bg-gray-700 rounded w-3/4"></div>
          <div className="space-y-2">
            <div className="h-4 bg-gray-700 rounded"></div>
            <div className="h-4 bg-gray-700 rounded w-5/6"></div>
          </div>
        </div>
      </div>
      <p className="mt-4 text-sm text-gray-400">Tracking active seller conversations, live intent transitions, AI confidence, and escalations...</p>
    </div>
  );
};
