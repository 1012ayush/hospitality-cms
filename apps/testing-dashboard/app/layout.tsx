import "./globals.css"; import type { Metadata } from "next";
export const metadata:Metadata={title:"Stayflow · Channel Control",description:"Internal hotel channel manager testing dashboard"};
export default function RootLayout({children}:{children:React.ReactNode}){return <html lang="en"><body>{children}</body></html>}
