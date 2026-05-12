import React from 'react';

export const AcquisitionsHeatMap: React.FC = () => {
  return (
    <div className="p-4 bg-gray-900 text-white rounded-lg shadow-md border border-gray-700">
      <h2 className="text-xl font-bold mb-4 text-orange-400">Acquisitions Heat Map</h2>
      <div className="h-48 bg-gray-800 rounded flex items-center justify-center border border-gray-700">
        <span className="text-gray-500">[ Geo-Spatial Visualization Pending ]</span>
      </div>
      <p className="mt-4 text-sm text-gray-400">Visualizing market activity, seller density, distress concentration, and response rates...</p>
    </div>
  );
};
