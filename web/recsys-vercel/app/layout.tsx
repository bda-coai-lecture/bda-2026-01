import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "BDA GitHub Recsys",
  description: "Vercel frontend for the local GitHub recommendation API",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
