import React from 'react';
import { SellerMemoryState } from '../../types/nexus';

export const SellerIntelligencePanel: React.FC = () => {
  return (
    <div className="p-4 bg-gray-900 text-white rounded-lg shadow-md border border-gray-700">
      <h2 className="text-xl font-bold mb-4 text-emerald-400">Seller Intelligence Panel</h2>
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-800 p-2 rounded">
            <h3 className="text-sm text-gray-400 uppercase tracking-wider">Acquisition Prob</h3>
            <div className="text-2xl font-bold">--%</div>
        </div>
        <div className="bg-gray-800 p-2 rounded">
            <h3 className="text-sm text-gray-400 uppercase tracking-wider">Distress Indicator</h3>
            <div className="text-2xl font-bold text-red-400">--/10</div>
        </div>
      </div>
      <p className="mt-4 text-sm text-gray-400">Analyzing portfolio, distress signals, timeline, emotional state, and negotiation posture...</p>
    </div>
  );
};
