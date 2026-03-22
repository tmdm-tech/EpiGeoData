import 'package:flutter/material.dart';

void main() {
  runApp(const EpigeoDataApp());
}

class EpigeoDataApp extends StatelessWidget {
  const EpigeoDataApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'EpigeoData',
      home: Scaffold(
        backgroundColor: const Color(0xFFF3F0FF),
        body: SafeArea(
          child: Center(
            child: Padding(
              padding: const EdgeInsets.all(24),
              child: ConstrainedBox(
                constraints: const BoxConstraints(maxWidth: 760),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: const [
                    Text(
                      'epigeodata',
                      style: TextStyle(
                        fontSize: 34,
                        fontWeight: FontWeight.w800,
                        letterSpacing: 0.3,
                        color: Color(0xFF4B2A9E),
                      ),
                    ),
                    SizedBox(height: 16),
                    Text(
                      'Camadas prontas para publicacao',
                      style: TextStyle(
                        fontSize: 22,
                        fontWeight: FontWeight.w700,
                        color: Color(0xFF1D1333),
                      ),
                    ),
                    SizedBox(height: 12),
                    Text(
                      'Checklist para execucao',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w500,
                        color: Color(0xFF3E3260),
                      ),
                    ),
                  ],
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
