import type { Metadata } from 'next';
import './globals.css';
import Sidebar from '@/components/Sidebar';

export const metadata: Metadata = {
  title: 'Threat Intelligence Dashboard',
  description: 'Real-time threat intelligence and malicious domain detection',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-slate-950">
        <div className="flex min-h-screen">
          <Sidebar />
          <main className="flex-1 ml-72 p-8 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}

