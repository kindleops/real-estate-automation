import React from 'react';
import { LiveNegotiationRadar } from './LiveNegotiationRadar';
import { SellerIntelligencePanel } from './SellerIntelligencePanel';
import { ConversationTimeline } from './ConversationTimeline';
import { AutonomousRoutingCenter } from './AutonomousRoutingCenter';
import { AcquisitionsHeatMap } from './AcquisitionsHeatMap';
import { AIOperationsMonitor } from './AIOperationsMonitor';

/**
 * Nexus Dashboard - Command Center View
 * Phase 3: Autonomous Acquisitions Operating System
 */
export const NexusDashboard: React.FC = () => {
  return (
    <div className="min-h-screen bg-black p-6 font-sans">
      <header className="mb-8 border-b border-gray-800 pb-4 flex justify-between items-end">
        <div>
            <h1 className="text-3xl font-bold text-white tracking-widest uppercase">Nexus Command</h1>
            <p className="text-gray-500 text-sm mt-1">Autonomous Acquisitions Operating System</p>
        </div>
        <div className="text-right">
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-900 text-green-300">
                System Online
            </span>
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Left Column */}
        <div className="space-y-6 flex flex-col">
          <LiveNegotiationRadar />
          <AutonomousRoutingCenter />
          <AIOperationsMonitor />
        </div>

        {/* Middle Column */}
        <div className="space-y-6 flex flex-col">
          <ConversationTimeline />
        </div>

        {/* Right Column */}
        <div className="space-y-6 flex flex-col">
          <SellerIntelligencePanel />
          <AcquisitionsHeatMap />
        </div>
      </div>
    </div>
  );
};

export default NexusDashboard;
